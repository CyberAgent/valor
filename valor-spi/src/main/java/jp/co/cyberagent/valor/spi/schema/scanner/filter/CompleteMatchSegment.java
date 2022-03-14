package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class CompleteMatchSegment extends SingleValueFilterSegment {

  public CompleteMatchSegment(PredicativeExpression origin, byte[] value) {
    super(origin, value);
  }

  @Override
  public SingleValueFilterSegment copyWithNewValue(byte[] value) {
    return new CompleteMatchSegment(origin, value);
  }

  @Override
  public String toString() {
    return String.format("= %s", ByteUtils.toString(this.value));
  }

  @Override
  public Type type() {
    return Type.COMPLETE_MATCH;
  }

  @Override
  public FilterSegment mergeByAnd(FilterSegment otherFragment) {
    return otherFragment.evaluate(value) ? this : FalseSegment.INSTANCE;
  }

  @Override
  public boolean evaluate(byte[] value) {
    return ByteUtils.equals(this.value, value);
  }
}
