package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import java.util.Arrays;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class BetweenSegment implements FilterSegment {

  class Border {

    public final byte[] value;
    public final boolean inclusive;

    public Border(byte[] value, boolean inclusive) {
      this.value = value;
      this.inclusive = inclusive;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Border)) {
        return false;
      }
      Border border = (Border) o;
      return inclusive == border.inclusive && Arrays.equals(value, border.value);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(inclusive);
      result = 31 * result + Arrays.hashCode(value);
      return result;
    }
  }


  private static Border andMin(Border l, Border r) {
    final int c = ByteUtils.compareTo(l.value, r.value);
    if (c == 0) {
      return l.inclusive ? r : l;
    }
    return c < 0 ? r : l;
  }

  private static Border andMax(Border l, Border r) {
    final int c = ByteUtils.compareTo(l.value, r.value);
    if (c == 0) {
      return l.inclusive ? r : l;
    }
    return c > 0 ? r : l;
  }

  private static Border orMin(Border l, Border r) {
    final int c = ByteUtils.compareTo(l.value, r.value);
    if (c == 0) {
      return l.inclusive ? l : r;
    }
    return c < 0 ? l : r;
  }

  private static Border orMax(Border l, Border r) {
    final int c = ByteUtils.compareTo(l.value, r.value);
    if (c == 0) {
      return l.inclusive ? l : r;
    }
    return c > 0 ? l : r;
  }

  private final transient PredicativeExpression origin;
  private final Border min;
  private final Border max;

  public BetweenSegment(PredicativeExpression origin,
                        boolean includeMin, byte[] min, boolean includeMax, byte[] max) {
    this.origin = origin;
    this.min = new Border(min, includeMin);
    this.max = new Border(max, includeMax);
  }

  public BetweenSegment(PredicativeExpression origin, Border min, Border max) {
    this.origin = origin;
    this.min = min;
    this.max = max;
  }

  @Override
  public PredicativeExpression getOrigin() {
    return origin;
  }

  public byte[] getMin() {
    return min.value;
  }

  public byte[] getMax() {
    return max.value;
  }

  public boolean isIncludeMin() {
    return min.inclusive;
  }

  public boolean isIncludeMax() {
    return max.inclusive;
  }

  @Override
  public Type type() {
    return Type.BETWEEN;
  }

  @Override
  public FilterSegment mergeByAnd(FilterSegment otherSegment) {
    PredicativeExpression mergedOrigin = AndOperator.join(this.origin, otherSegment.getOrigin());
    switch (otherSegment.type()) {
      case TRUE:
        return this;
      case FALSE:
        return otherSegment;
      case BETWEEN:
        BetweenSegment os = (BetweenSegment) otherSegment;
        return new BetweenSegment(mergedOrigin, andMin(min, os.min), andMax(max, os.max));
      case COMPLETE_MATCH:
        return evaluate(((CompleteMatchSegment)otherSegment).getValue())
            ? otherSegment : FalseSegment.INSTANCE;
      case LESS_THAN:
        LessThanSegment lts = (LessThanSegment) otherSegment;
        if (ByteUtils.compareTo(this.min.value, lts.value) > 0) {
          return FalseSegment.INSTANCE;
        }
        final Border mergedMax = andMax(this.max, new Border(lts.value, lts.includeBorder));
        return new BetweenSegment(mergedOrigin, min, mergedMax);
      case GREATER_THAN:
        GreaterThanSegment gts = (GreaterThanSegment)otherSegment;
        if (ByteUtils.compareTo(this.max.value, gts.value) < 0) {
          return FalseSegment.INSTANCE;
        }
        final Border mergedMin = andMin(this.min, new Border(gts.value, gts.includeBorder));
        return new BetweenSegment(mergedOrigin, mergedMin, max);
      case NOT_MATCH:
      case REGEXP:
        return this;
      default:
    }
    throw new IllegalArgumentException("unsupported segment type " + otherSegment);
  }

  @Override
  public boolean evaluate(byte[] value) {
    int compareMin = ByteUtils.compareTo(min.value, value);
    if (compareMin < 0 || (compareMin == 0 && min.inclusive)) {
      int compareMax = ByteUtils.compareTo(value, max.value);
      return compareMax < 0 || (compareMax == 0 && max.inclusive);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("between%s%s,%s%s",
        min.inclusive ? "[" : "(",
        ByteUtils.toHexString(min.value),
        ByteUtils.toHexString(max.value),
        max.inclusive ? "]" : ")"
    );
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BetweenSegment)) {
      return false;
    }
    BetweenSegment that = (BetweenSegment) o;
    return Objects.equals(min, that.min) && Objects.equals(max, that.max);
  }

  @Override
  public int hashCode() {
    return Objects.hash(min, max);
  }
}
