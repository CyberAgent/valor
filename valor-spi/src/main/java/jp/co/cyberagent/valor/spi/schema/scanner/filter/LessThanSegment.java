package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

/**
 *
 */
public class LessThanSegment extends SingleValueFilterSegment {

  protected final boolean includeBorder;

  public LessThanSegment(PredicativeExpression origin, byte[] value, boolean includeBorder) {
    super(origin, value);
    this.includeBorder = includeBorder;
  }

  @Override
  public Type type() {
    return Type.LESS_THAN;
  }

  @Override
  public FilterSegment mergeByAnd(FilterSegment otherFragment) {
    PredicativeExpression mergedOrigin = AndOperator.join(getOrigin(), otherFragment.getOrigin());
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
        int compare = ByteUtils.compareTo(lts.value, this.value);
        if (compare == 0) {
          return includeBorder ? otherFragment : this;
        }
        if (compare < 0) {
          return otherFragment;
        }
        return FalseSegment.INSTANCE;
      case GREATER_THAN:
        GreaterThanSegment gts = (GreaterThanSegment) otherFragment;
        return evaluate(gts.value)
            ? new BetweenSegment(mergedOrigin, gts.includeBorder, gts.value, includeBorder, value)
            : FalseSegment.INSTANCE;
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
        ? ByteUtils.compareTo(this.value, value) >= 0 : ByteUtils.compareTo(this.value, value) > 0;
  }

  public boolean isIncludeBorder() {
    return includeBorder;
  }

  @Override
  public SingleValueFilterSegment copyWithNewValue(byte[] value) {
    return new LessThanSegment(origin, value, includeBorder);
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
    LessThanSegment that = (LessThanSegment) o;
    return includeBorder == that.includeBorder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), includeBorder);
  }
}
