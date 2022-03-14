package jp.co.cyberagent.valor.spi.schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScanner;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;

/**
 * Keep prevType-less to be thread safe
 */
public interface Schema {

  String SELECT_REQUIRED = "valor.select.required";

  static void checkSchemaId(String relId, String schemaId) {
    if (schemaId == null) {
      throw new IllegalArgumentException("schemaId is null");
    }
    if (schemaId.isEmpty()) {
      throw new IllegalArgumentException("schemaId is empty");
    }
    if (schemaId.equals(relId)) {
      throw new IllegalArgumentException(
          "schemaId must differ from relationID (registered for the set of all schemata)");
    }
  }

  TupleDeserializer getTupleDeserializer(Relation relation)
      throws ValorException;

  TupleSerializer getTupleSerializer();

  QuerySerializer getQuerySerializer();

  /**
   * convert a Tuple to a list of KeyValues
   * TODO remove relation
   *
   * @throws IOException
   * @throws ValorException
   */
  List<Record> serialize(Tuple tuple) throws IOException, ValorException;

  SchemaScanner getScanner(SchemaScan scan, StorageConnection conn) throws ValorException;

  SchemaScan buildScan(Collection<String> attributes, PredicativeExpression condition)
      throws ValorException;

  default StorageMutation buildInsertMutation(Tuple... tuple) throws IOException, ValorException {
    return buildInsertMutation(Arrays.asList(tuple));
  }

  StorageMutation buildInsertMutation(Collection<Tuple> tuples) throws IOException, ValorException;

  default StorageMutation buildDeleteMutation(Tuple... tuple) throws IOException, ValorException {
    return buildDeleteMutation(Arrays.asList(tuple));
  }

  StorageMutation buildDeleteMutation(Collection<Tuple> tuples) throws IOException, ValorException;

  StorageMutation buildUpdateMutation(Tuple prev, Tuple post) throws IOException, ValorException;

  String getRelationId();

  String getSchemaId();

  Schema.Mode getMode();

  void setMode(Schema.Mode mode);

  Storage getStorage();

  StorageConnectionFactory getConnectionFactory();

  ValorConf getConf();

  void setConf(ValorConf conf);

  List<String> getFields();

  default Iterator<Segment> segmentIterator() {
    final Iterator<String> fields = getFields().iterator();
    final String initialField = fields.next();

    return new Iterator<Segment>() {

      private Iterator<Segment> segments = getLayout(initialField).formatters().iterator();

      @Override
      public boolean hasNext() {
        return segments.hasNext() || fields.hasNext();
      }

      @Override
      public Segment next() {
        String nextField = null;
        do {
          if (segments.hasNext()) {
            return segments.next();
          }
          nextField = fields.hasNext() ? fields.next() : null;
          segments = getLayout(nextField).formatters().iterator();
        } while (nextField != null);
        throw new NoSuchElementException();
      }
    };
  }

  FieldLayout getLayout(String fieldName);

  /**
   * check whether this schema definition contains the specified attribute
   */
  @Deprecated
  boolean containsAttribute(String attr);

  Collection<String> getContainedAttributes();

  boolean conBePrimary(Relation relation);

  boolean isPrimary();

  void setPrimary(boolean primary);

  enum Mode {
    PUBLIC {
      @Override
      public boolean isAllowed(Relation.OperationType opType) {
        return true;
      }
    }, READ_ONLY {
      @Override
      public boolean isAllowed(Relation.OperationType opType) {
        return Relation.OperationType.READ.equals(opType);
      }
    }, WRITE_ONLY {
      @Override
      public boolean isAllowed(Relation.OperationType opType) {
        return Relation.OperationType.WRITE.equals(opType);
      }
    };

    public abstract boolean isAllowed(Relation.OperationType opType);
  }
}
