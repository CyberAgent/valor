package jp.co.cyberagent.valor.spi.schema;

import java.util.Collection;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * make fragment slice-able
 */
public abstract class Holder implements Segment {
  static final Logger LOG = LoggerFactory.getLogger(Holder.class);

  protected Formatter formatter;

  public abstract TupleSerializer bytesWrapper(TupleSerializer wrapped) throws SerdeException;

  public abstract QuerySerializer filterWrapper(QuerySerializer wrapped) throws SerdeException;

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    formatter.accept(bytesWrapper(serializer), tuple);
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws ValorException {
    formatter.accept(filterWrapper(serializer), conjunction);
  }

  public Segment setFormatter(Formatter formatter) {
    this.formatter = formatter;
    return this;
  }

  @Override
  public Formatter getFormatter() {
    return formatter;
  }

  @Override
  public boolean containsAttribute(String attr) {
    return formatter.containsAttribute(attr);
  }
}
