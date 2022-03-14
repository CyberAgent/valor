package jp.co.cyberagent.valor.sdk.storage.kv;

import static jp.co.cyberagent.valor.spi.storage.StorageScan.UNSINGED_BYTES_COMPARATOR;


import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.exception.InvalidFieldException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

/**
 * in memory storage for test
 */
public class EmbeddedEKVStorageConnection implements KeyValueStorageConnection {

  private final EmbeddedEKVStorage storage;

  public EmbeddedEKVStorageConnection(EmbeddedEKVStorage storage) {
    this.storage = storage;
  }

  @Override
  public void insert(Collection<Record> records) throws ValorException {
    for (Record record : records) {
      byte[] key = record.getBytes(EmbeddedEKVStorage.KEY);
      byte[] col = record.getBytes(EmbeddedEKVStorage.COL);
      NavigableMap<byte[], byte[]> colVal = storage.get(key);
      if (colVal == null) {
        colVal = new TreeMap<>(UNSINGED_BYTES_COMPARATOR);
        storage.put(key, colVal);
      }
      colVal.put(col, record.getBytes(EmbeddedEKVStorage.VAL));
    }
  }

  @Override
  public void delete(Collection<Record> records) throws ValorException {
    for (Record record : records) {
      if (record.getBytes(EmbeddedEKVStorage.COL) == null) {
        storage.remove(record.getBytes(EmbeddedEKVStorage.KEY));
      } else {
        NavigableMap<byte[], byte[]> colVal = storage.get(record.getBytes(EmbeddedEKVStorage.KEY));
        if (colVal == null) {
          return;
        }
        colVal.remove(record.getBytes(EmbeddedEKVStorage.COL));
      }
    }
  }

  @Override
  public Optional<Long> count(SchemaScan scan) throws ValorException {
    throw new UnsupportedOperationException("count is not supported in " + getClass().getName());
  }

  @Override
  public StorageScanner getStorageScanner(StorageScan scan) {

    final Predicate<byte[]> keyFilter = buildFilter(scan.getFieldComparator(EmbeddedEKVStorage.KEY));
    final Predicate<byte[]> colFilter = buildFilter(scan.getFieldComparator(EmbeddedEKVStorage.COL));

    return new StorageScanner() {
      final Iterator<byte[]> keyItr = storage.keyIterator();
      byte[] key = null;
      Iterator<byte[]> colItr = null;

      public Record next() throws IOException {
        if (colItr == null) {
          if (!moveToNextCol()) {
            return null;
          }
        }
        while (colItr != null) {
          if (colItr.hasNext()) {
            byte[] col = colItr.next();
            Record r = new Record.RecordImpl();
            try {
              r.setBytes(EmbeddedEKVStorage.KEY, key);
              r.setBytes(EmbeddedEKVStorage.COL, col);
              if (scan.getFields().contains(EmbeddedEKVStorage.VAL)) {
                r.setBytes(EmbeddedEKVStorage.VAL, storage.get(key).get(col));
              }
              return r;
            } catch (InvalidFieldException e) {
              throw new IOException(e);
            }
          } else {
            moveToNextCol();
          }
        }
        return null;
      }

      protected boolean moveToNextCol() {
        if (!keyItr.hasNext()) {
          colItr = null;
          return false;
        }

        key = keyItr.next();
        if (keyFilter.test(key)) {
          NavigableMap<byte[], byte[]> col = storage.get(key);
          colItr = col.keySet().stream()
              .filter(colFilter).collect(Collectors.toList()).iterator();
          return true;
        } else {
          return moveToNextCol();
        }
      }

      @Override
      public void close() {
      }
    };
  }

  private Predicate<byte[]> buildFilter(FieldComparator comparator) {
    switch (comparator.getOperator()) {
      case EQUAL:
        return (b) -> Arrays.equals(b, comparator.getPrefix());
      case NOT_EQUAL:
        return (b) -> !Arrays.equals(b, comparator.getPrefix());
        // TODO replace Bytes.compareTo to Arrays.compare in JDK > 8
      case BETWEEN:
        byte[] start = comparator.getStart();
        byte[] stop = comparator.getStop();
        return stop == null ? (b) -> ByteUtils.compareTo(b, start) > 0
            : (b) -> ByteUtils.compareTo(b, start) > 0 && ByteUtils.compareTo(b, stop) < 0;
      case GREATER:
        return (b) -> ByteUtils.compareTo(b, comparator.getStart()) > 0;
      case LESS:
        return (b) -> ByteUtils.compareTo(b, comparator.getStop()) < 0;
      case REGEXP:
        return (b) -> true;
    }
    return null;
  }

  @Override
  public boolean isAvailable() {
    return true;
  }

  @Override
  public void close() {
  }
}
