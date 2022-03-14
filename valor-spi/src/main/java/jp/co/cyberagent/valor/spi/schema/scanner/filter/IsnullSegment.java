package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;

/**
 *
 */
public class IsnullSegment implements FilterSegment {

  private final PredicativeExpression origin;

  public IsnullSegment(PredicativeExpression origin) {
    this.origin = origin;
  }

  @Override
  public PredicativeExpression getOrigin() {
    return origin;
  }

  @Override
  public Type type() {
    return null;
  }

  @Override
  public FilterSegment mergeByAnd(FilterSegment otherFragment) {
    return this;
  }

  @Override
  public boolean evaluate(byte[] value) {
    throw new IllegalStateException("this is marker fragment and should not be invoked");
  }
}
