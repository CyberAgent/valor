package jp.co.cyberagent.valor.sdk.schema.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import jp.co.cyberagent.valor.sdk.serde.ContinuousRecordsDeserializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedQuerySerialzier;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.sdk.storage.kv.KeyValueStorageConnectionFactory;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
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

public class SortedMultiKeyValuesSchema extends SortedKeyValueSchema {

  public SortedMultiKeyValuesSchema(StorageConnectionFactory connectionFactory, Relation relation,
                                    SchemaDescriptor descriptor) {
    super(connectionFactory, relation, descriptor);
  }

  @Override
  public TupleDeserializer getTupleDeserializer(Relation relation) {
    return new ContinuousRecordsDeserializer(relation, layouts);
  }

  @Override
  public TupleSerializer getTupleSerializer() {
    return new TreeBasedTupleSerializer();
  }

  @Override
  public QuerySerializer getQuerySerializer() {
    return new TreeBasedQuerySerialzier();
  }

  @Override
  public SchemaScanner getScanner(SchemaScan scan, StorageConnection conn) throws ValorException {

    return new BaseSchemaScanner(scan, conn) {

      private ContinuousRecordsDeserializer deserializer;

      @Override
      protected Tuple flushRemainingData() throws ValorException {
        return deserializer.flushRemaining();
      }

      @SuppressWarnings("unchecked")
      // TODO consider test
      @Override
      protected Tuple readNextTuple(List<String> fields, boolean ignoreInvalidRecord)
          throws IOException, ValorException {
        if (deserializer == null) {
          deserializer = (ContinuousRecordsDeserializer) getTupleDeserializer(relation);
        }
        Record nextRecord;
        while ((nextRecord = readNextRecord()) != null) {
          try {
            deserializer.readRecord(fields, nextRecord);
            Tuple tuple = deserializer.pollTuple();
            if (tuple != null) {
              return tuple;
            }
          } catch (SerdeException e) {
            if (!ignoreInvalidRecord) {
              throw e;
            }
            LOG.warn("an invalid record is scanned", e);
          }
        }
        return null;
      }
    };
  }

  @Override
  public StorageMutation buildDeleteMutation(final Collection<Tuple> tuples) throws ValorException {
    return storageConnection -> {
      final Collection<Record> records = new ArrayList<>(tuples.size());
      for (Tuple tuple : tuples) {
        List<Record> rs = serialize(tuple);
        Record r = retainKeys(rs.stream().findFirst().get());
        records.add(r);
      }
      storageConnection.delete(records);
    };
  }

  @Override
  public StorageMutation buildUpdateMutation(Tuple prev, Tuple post) throws ValorException {
    return storageConnection -> {
      Collection<Record> prevRecords = serialize(prev);
      final Record prevRecord = prevRecords.stream().findFirst().get();
      final Collection<Record> records = serialize(post);
      storageConnection.delete(retainKeys(prevRecord));
      for (Record r : records) {
        storageConnection.insert(r);
      }
    };
  }

  private Record retainKeys(Record record) throws ValorException {
    List<String> fields = connectionFactory.getFields();
    List<String> rowkeyFields
        = ((KeyValueStorageConnectionFactory) connectionFactory).getRowkeyFields();
    for (String field : fields) {
      if (!rowkeyFields.contains(field)) {
        record.setBytes(field, null);
      }
    }
    return record;
  }
}
