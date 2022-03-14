package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import java.util.Arrays;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;

public abstract class SingleValueFilterSegment implements FilterSegment {

  protected final PredicativeExpression origin;

  protected final byte[] value;

  public SingleValueFilterSegment(PredicativeExpression origin, byte[] value) {
    this.origin = origin;
    this.value = value;
  }

  @Override
  public PredicativeExpression getOrigin() {
    return origin;
  }

  public byte[] getValue() {
    return value;
  }

  public abstract SingleValueFilterSegment copyWithNewValue(byte[] value);

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SingleValueFilterSegment that = (SingleValueFilterSegment) o;
    return Arrays.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(value);
  }
}
