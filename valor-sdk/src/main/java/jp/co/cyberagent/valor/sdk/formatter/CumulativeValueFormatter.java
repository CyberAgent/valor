package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;

public abstract class CumulativeValueFormatter<V> extends Formatter {
  public abstract Object aggregate(String attr, List<V> values) throws ValorException;

  public abstract Function<Relation, TupleDeserializer>
      getDeserializerFactory(String valueField, List<FieldLayout> layouts);

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) {
    throw new UnsupportedOperationException("accept for cumulative table is not supported");
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction) {
    serializer.write(null, TrueSegment.INSTANCE);
  }
}
