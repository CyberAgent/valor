package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class GreaterThanSegment extends SingleValueFilterSegment {

  protected final boolean includeBorder;

  public GreaterThanSegment(PredicativeExpression origin, byte[] value, boolean includeBorder) {
    super(origin, value);
    this.includeBorder = includeBorder;
  }

  @Override
  public Type type() {
    return Type.GREATER_THAN;
  }

  @Override
  public FilterSegment mergeByAnd(FilterSegment otherFragment) {
    PredicativeExpression mergedOrigin = AndOperator.join(origin, otherFragment.getOrigin());
    switch (otherFragment.type()) {
      case TRUE:
        return this;
      case FALSE:
        return otherFragment;
      case BETWEEN:
        return otherFragment.mergeByAnd(this);
      case COMPLETE_MATCH:
        return otherFragment.mergeByAnd(this);
      case LESS_THAN:
        LessThanSegment lts = (LessThanSegment) otherFragment;
        return evaluate(lts.value)
            ? new BetweenSegment(mergedOrigin, includeBorder, value, lts.includeBorder, lts.value)
            : FalseSegment.INSTANCE;
      case GREATER_THAN:
        GreaterThanSegment gts = (GreaterThanSegment) otherFragment;
        int compare = ByteUtils.compareTo(this.value, gts.value);
        if (compare == 0) {
          return includeBorder ? otherFragment : this;
        }
        if (compare < 0) {
          return otherFragment;
        }
        return FalseSegment.INSTANCE;
      case REGEXP:
      case NOT_MATCH:
        return this;
      default:
    }
    throw new IllegalArgumentException("unsupported segment type " + otherFragment);
  }

  @Override
  public boolean evaluate(byte[] value) {
    return includeBorder
        ? ByteUtils.compareTo(this.value, value) <= 0 : ByteUtils.compareTo(this.value, value) < 0;
  }

  public boolean isIncludeBorder() {
    return includeBorder;
  }

  @Override
  public SingleValueFilterSegment copyWithNewValue(byte[] value) {
    return new GreaterThanSegment(origin, value, includeBorder);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    GreaterThanSegment that = (GreaterThanSegment) o;
    return includeBorder == that.includeBorder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), includeBorder);
  }
}
