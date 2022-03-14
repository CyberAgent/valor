package jp.co.cyberagent.valor.sdk.schema.kv;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.formatter.CumulativeValueFormatter;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;

public class SortedCumulativeKeyValuesSchema extends SortedMultiKeyValuesSchema {

  private CumulativeValueFormatter cumulativeElement;

  private Function<Relation, TupleDeserializer> deserializerFactory;

  public SortedCumulativeKeyValuesSchema(
      StorageConnectionFactory connectionFactory, Relation relation,
      SchemaDescriptor descriptor) throws ValorException {
    super(connectionFactory, relation, descriptor);

    List<String> keyFields = connectionFactory.getKeyFields();
    List<String> fields = connectionFactory.getFields();
    List<String> valueFields =
        fields.stream().filter(((Predicate<String>) keyFields::contains).negate())
            .collect(Collectors.toList());
    if (valueFields.size() != 1) {
      new IllegalArgumentException("value filed expected to be 1 but " + valueFields.size());
    }
    String valueFieldName = valueFields.get(0);
    FieldLayout valueLayout =
        layouts.stream().filter(l -> valueFieldName.equals(l.getFieldName())).findFirst().get();

    if (valueLayout.getFormatters().size() != 1) {
      new IllegalArgumentException(
          "value formatter size expected to be 1 but " + valueFields.size());
    }
    cumulativeElement =
        (CumulativeValueFormatter) valueLayout.getFormatters().get(0).getFormatter();
    deserializerFactory = cumulativeElement.getDeserializerFactory(valueFieldName, layouts);
  }

  @Override
  public TupleSerializer getTupleSerializer() {
    return new TreeBasedTupleSerializer();
  }

  @Override
  public TupleDeserializer getTupleDeserializer(Relation relation) {
    return deserializerFactory.apply(relation);
  }

  @Override
  public StorageMutation buildDeleteMutation(final Collection<Tuple> tuples) throws ValorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public StorageMutation buildUpdateMutation(Tuple prev, Tuple post) throws ValorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public StorageMutation buildInsertMutation(Collection<Tuple> tuples) throws ValorException {
    throw new UnsupportedOperationException();
  }
}
