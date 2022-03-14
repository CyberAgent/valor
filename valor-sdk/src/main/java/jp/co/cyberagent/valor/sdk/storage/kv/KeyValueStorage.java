package jp.co.cyberagent.valor.sdk.storage.kv;

import java.util.List;
import jp.co.cyberagent.valor.sdk.formatter.CumulativeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.DisassembleFormatter;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedCumulativeKeyValuesSchema;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedMonoKeyValueSchema;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedMultiKeyValuesSchema;
import jp.co.cyberagent.valor.sdk.storage.StorageBase;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.IllegalSchemaException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;

public abstract class KeyValueStorage extends StorageBase {

  public KeyValueStorage(ValorConf conf) {
    super(conf);
  }

  @Override
  public Schema buildSchema(Relation relation, SchemaDescriptor descriptor) throws ValorException {
    boolean multiCell = false;
    boolean cumulative = false;
    List<FieldLayout> fields = descriptor.getFields();

    for (int i = 0; i < fields.size(); i++) {
      FieldLayout field = fields.get(i);
      for (Segment formatter : field.formatters()) {
        Segment elm = formatter.getFormatter();
        boolean splitted = elm instanceof DisassembleFormatter;
        cumulative = cumulative || elm instanceof CumulativeValueFormatter;
        if (i < getKeys().size()) {
          multiCell = multiCell || splitted;
          if (cumulative) {
            throw new IllegalSchemaException(relation.getRelationId(), descriptor.getSchemaId(),
                "cumulative element is not allowed in key");
          }
        }
      }
    }

    StorageConnectionFactory connectionFactory = getConnectionFactory(relation, descriptor);
    if (multiCell) {
      return cumulative ? new SortedCumulativeKeyValuesSchema(connectionFactory, relation,
          descriptor) :
          new SortedMultiKeyValuesSchema(connectionFactory, relation, descriptor);
    } else {
      return new SortedMonoKeyValueSchema(connectionFactory, relation, descriptor);
    }
  }

  protected abstract List<String> getKeys();

  protected abstract StorageConnectionFactory getConnectionFactory(Relation relation,
                                                                   SchemaDescriptor descriptor);
}
