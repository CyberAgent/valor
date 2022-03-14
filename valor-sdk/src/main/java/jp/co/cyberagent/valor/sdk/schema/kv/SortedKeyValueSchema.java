package jp.co.cyberagent.valor.sdk.schema.kv;

import java.util.ArrayList;
import java.util.Collection;
import jp.co.cyberagent.valor.sdk.formatter.AggregationFormatter;
import jp.co.cyberagent.valor.sdk.schema.SchemaBase;
import jp.co.cyberagent.valor.sdk.serde.DisassembleDeserializer;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;

public abstract class SortedKeyValueSchema extends SchemaBase {

  private boolean includedNestedValues = false;

  public SortedKeyValueSchema(StorageConnectionFactory connectionFactory,
                              Relation relation,
                              SchemaDescriptor descriptor) {
    super(connectionFactory, relation, descriptor);
    for (FieldLayout field :descriptor.getFields()) {
      for (Segment segment : field.formatters()) {
        if (segment.getFormatter() instanceof AggregationFormatter) {
          includedNestedValues = true;
        }
      }
    }
  }

  @Override
  public TupleDeserializer getTupleDeserializer(Relation relation) {
    if (includedNestedValues) {
      return new DisassembleDeserializer(relation, layouts);
    } else {
      return new OneToOneDeserializer(relation, layouts);
    }
  }

  @Override
  public StorageMutation buildInsertMutation(Collection<Tuple> tuples) throws ValorException {
    return storageConnection -> {
      // TODO set array size
      Collection<Record> records = new ArrayList<>();
      for (Tuple tuple : tuples) {
        records.addAll(serialize(tuple));
      }
      storageConnection.insert(records);
    };
  }
}
