package jp.co.cyberagent.valor.sdk.schema.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import jp.co.cyberagent.valor.sdk.formatter.SaltFormatter;
import jp.co.cyberagent.valor.sdk.serde.OneToOneQuerySerializer;
import jp.co.cyberagent.valor.sdk.serde.OneToOneTupleSerializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedQuerySerialzier;
import jp.co.cyberagent.valor.spi.conf.ValorConfParam;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.schema.scanner.BaseSchemaScanner;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScanner;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;
import jp.co.cyberagent.valor.spi.util.Closer;

public class SortedMonoKeyValueSchema extends SortedKeyValueSchema {

  public static final ValorConfParam  PARALLEL_MERGE_INIT_THREAD
      = new ValorConfParam("valor.parallelmerge.init.threads",
      Integer.toString(Runtime.getRuntime().availableProcessors()));

  private final boolean salted;

  public SortedMonoKeyValueSchema(
      StorageConnectionFactory connectionFactory,
      Relation relation,
      SchemaDescriptor descriptor) {
    super(connectionFactory, relation, descriptor);
    salted = descriptor.getFields().stream()
        .flatMap(l -> l.formatters().stream())
        .filter(s -> s instanceof SaltFormatter).findAny().isPresent();
  }

  @Override
  public TupleSerializer getTupleSerializer() {
    return new OneToOneTupleSerializer();
  }

  @Override
  public QuerySerializer getQuerySerializer() {
    if (salted) {
      return new TreeBasedQuerySerialzier();
    } else {
      return new OneToOneQuerySerializer();
    }
  }

  @Override
  public SchemaScanner getScanner(SchemaScan scan, StorageConnection conn) throws ValorException {
    if (salted && scan.getFragments().size() > 1) {
      return new ParallelMergeRunner(scan, conn);
    } else {
      return new NaiveSchemaScanner(scan, conn);
    }
  }

  @Override
  public StorageMutation buildInsertMutation(Collection<Tuple> tuples) throws ValorException {
    return storageConnectione -> {
      // TODO set array size
      Collection<Record> records = new ArrayList<>();
      for (Tuple tuple : tuples) {
        records.addAll(serialize(tuple));
      }
      storageConnectione.insert(records);
    };
  }

  @Override
  public StorageMutation buildDeleteMutation(Collection<Tuple> tuples) throws ValorException {
    return storageConnection -> {
      final Collection<Record> records = new ArrayList<>(tuples.size());
      for (Tuple tuple : tuples) {
        Collection<Record> rs = serialize(tuple);
        if (rs.size() != 1) {
          throw new IllegalStateException("unexpected record size " + records.toString());
        }
        records.add(rs.stream().findFirst().get());
      }
      storageConnection.delete(records);
    };
  }

  @Override
  public StorageMutation buildUpdateMutation(Tuple prev, Tuple post) throws IOException,
      ValorException {
    return buildInsertMutation(post);
  }

  private class NaiveSchemaScanner extends BaseSchemaScanner {
    private TupleDeserializer deserializer;

    public NaiveSchemaScanner(SchemaScan scan, StorageConnection conn) throws ValorException {
      super(scan, conn);
      deserializer = getTupleDeserializer(relation);
    }

    @Override
    protected Tuple flushRemainingData() {
      return null;
    }

    @Override
    protected Tuple readNextTuple(List<String> fields, boolean ignoreInvalidRecord)
        throws ValorException, IOException {
      Tuple t = deserializer.pollTuple();
      if (t != null) {
        return t;
      }
      Record record;
      while ((record = readNextRecord()) != null) {
        if (record == null) {
          return null;
        }
        try {
          deserializer.readRecord(fields, record);
          return deserializer.pollTuple();
        } catch (SerdeException e) {
          if (!ignoreInvalidRecord) {
            throw e;
          }
          LOG.warn("an invalid record is scanned", e);
        }
      }
      return null;
    }
  }

  private class ParallelMergeRunner extends NaiveSchemaScanner {

    // TODO check validity with salt range
    private final int outputCapacity = 1280;
    private final int batchSize = 5;

    private final LinkedBlockingQueue<Record> outputQueue;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final Future<Void> pollerThread;

    class Poller implements Callable<Void> {
      private PriorityBlockingQueue<IndexedRecord> sortQueue;
      private int[] numRecordsInSortQueue;
      private StorageScanner[] scanners;
      private boolean completed = false;

      public Poller(IgnoreSaltComparator comparator) {
        this.sortQueue = new PriorityBlockingQueue<>(batchSize * scans.size(), comparator);
        this.numRecordsInSortQueue = new int[scans.size()];
        this.scanners = new StorageScanner[scans.size()];
        initialScan();
      }

      @Override
      public Void call() throws Exception {
        while (!completed && !Thread.currentThread().isInterrupted()) {
          final IndexedRecord record = sortQueue.poll();
          while (
              !outputQueue.offer(record.record, 100, TimeUnit.MILLISECONDS)
              && !Thread.currentThread().isInterrupted()) {
          }
          --numRecordsInSortQueue[record.index];
          if (numRecordsInSortQueue[record.index] < 0) {
            continue;
          } else if (numRecordsInSortQueue[record.index] == 0) {
            int numRecords = executeScanBatch(record.index);
            if (numRecords == 0) {
              this.scanners[record.index].close();
              this.scanners[record.index] = null;
              completed = !Arrays.stream(scanners).anyMatch(s -> s != null);
              if (completed) {
                IndexedRecord r;
                while ((r = sortQueue.poll()) != null && !Thread.currentThread().isInterrupted()) {
                  while (!outputQueue.offer(r.record, 100, TimeUnit.MILLISECONDS)
                      && !Thread.currentThread().isInterrupted()) {
                  }
                }
              }
            } else {
              this.numRecordsInSortQueue[record.index] += numRecords;
            }
          }
        }
        // finalize
        Closer closer = new Closer();
        IntStream.range(0, scanners.length)
            .filter(i -> scanners[i] != null).forEach(i -> closer.close(scanners[i]));
        closer.throwIfFailed();
        return null;
      }

      private void initialScan() {
        int parallelism = Integer.parseInt(PARALLEL_MERGE_INIT_THREAD.get(conf));
        parallelism = Math.min(scans.size(), parallelism);
        ExecutorService initiator = Executors.newFixedThreadPool(parallelism);
        Future<Integer>[] futures = new Future[scans.size()];
        try {
          boolean exist = false;
          for (int i = 0; i < scans.size(); i++) {
            this.scanners[i] = conn.getStorageScanner(scans.get(i));
            futures[i] = initiator.submit(new ScanBatchCallable(i));
          }
          for (int i = 0; i < scans.size(); i++) {
            int numRecords = futures[i].get();
            if (numRecords == 0) {
              this.numRecordsInSortQueue[i] = -1;
              this.scanners[i].close();
              this.scanners[i] = null;
            } else {
              this.numRecordsInSortQueue[i] = numRecords;
              exist = true;
            }
          }
          if (!exist) {
            completed = true;
          }
        } catch (IOException | ValorException | ExecutionException | InterruptedException e) {
          throw new IllegalStateException(e);
        } finally {
          initiator.shutdown();
          try {
            if (!initiator.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
              throw new IllegalStateException("failed to shudown initiation thread pool (timeout)");
            }
          } catch (InterruptedException e) {
            throw new IllegalStateException("failed to shudown initiation thread pool", e);
          }
        }
      }

      private int executeScanBatch(int index) throws IOException {
        StorageScanner scanner = this.scanners[index];
        for (int i = 0; i < batchSize; i++) {
          Record record = scanner.next();
          if (record == null) {
            return i;
          }
          sortQueue.add(new IndexedRecord(index, record));
        }
        return batchSize;
      }

      class ScanBatchCallable implements Callable<Integer> {
        private final int idx;

        ScanBatchCallable(int idx) {
          this.idx = idx;
        }

        @Override
        public Integer call() throws Exception {
          return executeScanBatch(idx);
        }
      }
    }

    public ParallelMergeRunner(SchemaScan scan, StorageConnection conn) throws ValorException {
      super(scan, conn);
      Schema schema = scan.getSchema();
      String keyField = schema.getFields().get(0);
      FieldLayout keyLayout = schema.getLayout(keyField);
      Segment saltFormatter = keyLayout.formatters().get(0);
      int saltSize = saltFormatter instanceof SaltFormatter
          ? ((SaltFormatter) saltFormatter).getSaltSize() : 0;
      this.outputQueue = new LinkedBlockingQueue<>(outputCapacity);
      Callable<Void> poller = new Poller(new IgnoreSaltComparator(saltSize, keyField));
      this.pollerThread = executor.submit(poller);
    }


    @Override
    protected Record readNextRecord() throws IOException, ValorException {
      if (pollerThread.isDone()) {
        try {
          pollerThread.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new IOException(e);
        }
      }
      try {
        do {
          Record record = outputQueue.poll(100, TimeUnit.MILLISECONDS);
          if (record != null) {
            return record;
          }
        } while (!pollerThread.isDone() || !outputQueue.isEmpty());
      } catch (InterruptedException e) {
        throw new IOException();
      }
      return null;
    }

    @Override
    public void close() throws IOException {
      if (!pollerThread.isDone()) {
        pollerThread.cancel(true);
      }
      super.close();
      executor.shutdown();
    }
  }

  static class IndexedRecord {
    public final int index;
    public final Record record;

    IndexedRecord(int index, Record record) {
      this.index = index;
      this.record = record;
    }
  }

  static class IgnoreSaltComparator implements Comparator<IndexedRecord> {

    private final int saltSize;
    private final String keyField;

    IgnoreSaltComparator(int saltSize, String keyField) {
      this.saltSize = saltSize;
      this.keyField = keyField;
    }

    @Override
    public int compare(IndexedRecord o1, IndexedRecord o2) {
      try {
        return compare(o1.record.getBytes(keyField), o2.record.getBytes(keyField));
      } catch (ValorException e) {
        throw new IllegalStateException(e);
      }

    }

    public int compare(byte[] key1, byte[] key2) {
      for (int i = saltSize, j = saltSize; i < key1.length && j < key2.length; i++, j++) {
        int a = (key1[i] & 0xff);
        int b = (key2[j] & 0xff);
        if (a != b) {
          return a - b;
        }
      }
      return key1.length - key2.length;
    }
  }


}
