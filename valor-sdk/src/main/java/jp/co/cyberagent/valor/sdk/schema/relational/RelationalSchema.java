package jp.co.cyberagent.valor.sdk.schema.relational;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import jp.co.cyberagent.valor.sdk.schema.SchemaBase;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedQuerySerialzier;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.sdk.storage.relational.RelationalStorageConnection;
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

/**
 *
 */
public class RelationalSchema extends SchemaBase {

  public RelationalSchema(StorageConnectionFactory connectionFactory,
                          Relation relation,
                          SchemaDescriptor schemaDescriptor) {
    super(connectionFactory, relation, schemaDescriptor);
  }

  @Override
  public TupleDeserializer getTupleDeserializer(Relation relation) {
    return new OneToOneDeserializer(relation, layouts);
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
      @Override
      protected Tuple flushRemainingData() {
        return null;
      }

      @Override
      protected Tuple readNextTuple(List<String> fields, boolean skipInvalidRecord)
          throws IOException, ValorException {
        // FIXME reuse deserializer INSTANCE
        TupleDeserializer deserializer = getTupleDeserializer(relation);
        Record record;
        while ((record = readNextRecord()) != null) {
          try {
            deserializer.readRecord(fields, record);
            return deserializer.pollTuple();
          } catch (SerdeException e) {
            if (!skipInvalidRecord) {
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
  public StorageMutation buildInsertMutation(Collection<Tuple> tuples) throws ValorException {
    return storageConnection -> {
      final Collection<Record> records = new ArrayList<>(tuples.size());
      for (Tuple tuple : tuples) {
        Collection<Record> rs = serialize(tuple);
        if (rs.size() != 1) {
          throw new IllegalStateException("unexpected record size " + records.toString());
        }
        records.add(rs.stream().findFirst().get());
      }
      storageConnection.insert(records);
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
  public StorageMutation buildUpdateMutation(Tuple prev, Tuple post) throws ValorException {
    return storageConnection -> {
      Collection<Record> records = serialize(post);
      if (records.size() != 1) {
        throw new IllegalStateException("unexpected record size " + records.toString());
      }
      final Record record = records.stream().findFirst().get();
      ((RelationalStorageConnection) storageConnection).update(record);
    };
  }
}
