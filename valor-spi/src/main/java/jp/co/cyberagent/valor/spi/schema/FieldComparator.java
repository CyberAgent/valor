package jp.co.cyberagent.valor.spi.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.BetweenSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.NotMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.RegexpMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.util.ByteUtils;


/**
 *
 */
public class FieldComparator {

  public enum Operator {
    HEAD, EQUAL, NOT_EQUAL, BETWEEN, GREATER, LESS, REGEXP
  }

  private Operator operator = Operator.HEAD;
  // TODO use buffer
  private byte[] prefix = new byte[0];
  private byte[] regexp = new byte[0];
  // inclusive start row
  private byte[] start;
  // exclusive stop row
  private byte[] stop;

  private transient List<PredicativeExpression> embeddedPredicates = new ArrayList<>();

  public static FieldComparator build(Operator op, byte[] prefix, byte[] start, byte[] stop,
                                      byte[] regexp) {
    FieldComparator comparator = new FieldComparator();
    comparator.operator = op;
    comparator.prefix = prefix;
    comparator.start = start;
    comparator.stop = stop;
    comparator.regexp = regexp;
    return comparator;
  }

  public static FieldComparator build(FieldComparator org) {
    FieldComparator comparator = new FieldComparator();
    comparator.operator = org.operator;
    comparator.prefix = org.prefix;
    comparator.start = org.start;
    comparator.stop = org.stop;
    comparator.regexp = org.regexp;
    return comparator;
  }

  private static int compare(byte[] left, byte[] right) {
    return StorageScan.UNSINGED_BYTES_COMPARATOR.compare(left, right);
  }

  public Operator getOperator() {
    return operator;
  }

  public byte[] getRegexp() {
    if (regexp == null || regexp.length == 0
        || ByteUtils.equals(regexp, ByteUtils.add(prefix, RegexpMatchSegment.WILDCARD))) {
      return null;
    }
    return regexp;
  }

  public byte[] getPrefix() {
    return prefix;
  }

  public byte[] getStart() {
    return start == null ? StorageScan.START_BYTES : start;
  }

  public byte[] getStop() {
    return stop == null ? StorageScan.END_BYTES : stop;
  }

  public List<PredicativeExpression> getEmbeddedPredicates() {
    return embeddedPredicates;
  }

  /**
   * @return a field comparator to get all record retrieved by this and given field comparators
   */
  public FieldComparator mergeByOr(FieldComparator other) {
    if (isSameEqaulComparators(other)) {
      return this;
    }
    FieldComparator merged = new FieldComparator();
    merged.operator = Operator.BETWEEN;
    merged.start = compare(this.start, other.start) < 0 ? this.start : other.start;
    merged.stop = compare(this.stop, other.stop) < 0 ? other.stop : this.stop;
    int s = Math.min(this.embeddedPredicates.size(), other.embeddedPredicates.size());
    IntStream.range(0, s).forEach(i ->
        merged.embeddedPredicates.add(
            OrOperator.join(this.embeddedPredicates.get(i), other.embeddedPredicates.get(i))));
    return merged;
  }

  private boolean isSameEqaulComparators(FieldComparator other) {
    if (!Operator.EQUAL.equals(this.operator)) {
      return false;
    }
    if (!Operator.EQUAL.equals(other.operator)) {
      return false;
    }
    return StorageScan.UNSINGED_BYTES_COMPARATOR.compare(this.prefix, other.prefix) == 0;
  }

  /**
   * add filter fragment to this comparator
   * @param fragment
   * @return ture if the embedded predicate is pushed down into this comparator
   */
  public boolean append(FilterSegment fragment) {
    boolean pusheddown = false;
    switch (fragment.type()) {
      case TRUE:
        appendTrueFragment((TrueSegment) fragment);
        break;
      case FALSE:
        break;
      case BETWEEN:
        pusheddown = appendBetweenFragment((BetweenSegment) fragment);
        break;
      case COMPLETE_MATCH:
        pusheddown = appendCompleteFragment((CompleteMatchSegment) fragment);
        break;
      case LESS_THAN:
        pusheddown = appendLessThanFragment((LessThanSegment) fragment);
        break;
      case GREATER_THAN:
        pusheddown = appendGreaterThanFragment((GreaterThanSegment) fragment);
        break;
      case REGEXP:
        pusheddown = appendRegexpMatchFragment((RegexpMatchSegment) fragment);
        break;
      case NOT_MATCH:
        pusheddown = appendNotMatchFragment((NotMatchSegment) fragment);
        break;
      default:
        throw new IllegalArgumentException(fragment + " is not supported");
    }
    if (pusheddown) {
      final PredicativeExpression embeddedPredicate = fragment.getOrigin();
      if (embeddedPredicate != null) {
        // ignore null which is embedded by salt formatters.
        embeddedPredicates.add(embeddedPredicate);
      }
    }
    return pusheddown;
  }

  private boolean appendCompleteFragment(CompleteMatchSegment fragment) {
    if (isStringMatchFragment(fragment)) {
      regexp = ByteUtils.add(regexp, fragment.getValue());
    } else {
      regexp = addWildcard(regexp);
    }
    switch (operator) {
      case HEAD:
        fixStartAndStopRow();
        operator = Operator.EQUAL;
        prefix = ByteUtils.add(prefix, fragment.getValue());
        fixStartAndStopRow();
        return true;
      case EQUAL:
        prefix = ByteUtils.add(prefix, fragment.getValue());
        fixStartAndStopRow();
        return true;
      case NOT_EQUAL:
      case BETWEEN:
      case GREATER:
      case LESS:
      case REGEXP:
      default:
    }
    return false;
  }

  private boolean isStringMatchFragment(CompleteMatchSegment segment) {
    PredicativeExpression pred = segment.getOrigin();
    if (pred instanceof BinaryPrimitivePredicate) {
      BinaryPrimitivePredicate bpp = (BinaryPrimitivePredicate) pred;
      if (bpp.getLeft() instanceof AttributeNameExpression) {
        if (bpp.getLeft().getType() instanceof StringAttributeType) {
          return true;
        }
      }
      if (bpp.getRight() instanceof AttributeNameExpression) {
        if (bpp.getRight().getType() instanceof StringAttributeType) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean appendNotMatchFragment(NotMatchSegment fragment) {
    regexp = addWildcard(regexp);
    switch (operator) {
      case HEAD:
        operator = Operator.REGEXP;
        break;
      case EQUAL:
        fixStartAndStopRow();
        operator = Operator.BETWEEN;
        break;
      case NOT_EQUAL:
      case BETWEEN:
      case GREATER:
      case LESS:
      case REGEXP:
      default:
    }
    return false;
  }

  private boolean appendBetweenFragment(BetweenSegment fragment) {
    regexp = addWildcard(regexp);
    switch (operator) {
      case HEAD:
      case EQUAL:
        fixStartAndStopRow();
        start = ByteUtils.add(prefix, fragment.getMin());
        stop = ByteUtils.add(prefix, fragment.getMax());
        if (fragment.isIncludeMax()) {
          stop = incrementAsRange(stop);
        }
        operator = Operator.BETWEEN;
        return true;
      case NOT_EQUAL:
      case BETWEEN:
      case GREATER:
      case LESS:
      case REGEXP:
      default:
    }
    return false;
  }

  private boolean appendGreaterThanFragment(GreaterThanSegment fragment) {
    regexp = addWildcard(regexp);
    switch (operator) {
      case HEAD:
        start = ByteUtils.add(prefix, fragment.getValue());
        operator = Operator.GREATER;
        return true;
      case EQUAL:
        fixStartAndStopRow();
        start = ByteUtils.add(prefix, fragment.getValue());
        operator = Operator.BETWEEN;
        return true;
      case LESS:
      case NOT_EQUAL:
      case BETWEEN:
      case GREATER:
      case REGEXP:
      default:
    }
    return false;
  }

  private boolean appendLessThanFragment(LessThanSegment fragment) {
    regexp = addWildcard(regexp);
    switch (operator) {
      case HEAD:
        stop = ByteUtils.add(prefix, fragment.getValue());
        if (fragment.isIncludeBorder()) {
          stop = incrementAsRange(stop);
        }
        operator = Operator.LESS;
        return true;
      case EQUAL:
        fixStartAndStopRow();
        stop = ByteUtils.add(prefix, fragment.getValue());
        if (fragment.isIncludeBorder()) {
          stop = incrementAsRange(stop);
        }
        operator = Operator.BETWEEN;
        return true;
      case GREATER:
      case NOT_EQUAL:
      case BETWEEN:
      case LESS:
      case REGEXP:
      default:
    }
    return false;
  }

  private boolean appendRegexpMatchFragment(RegexpMatchSegment fragment) {
    regexp = ByteUtils.add(regexp, fragment.getValue());
    switch (operator) {
      case HEAD:
        operator = Operator.REGEXP;
        break;
      case EQUAL:
        fixStartAndStopRow();
        operator = Operator.BETWEEN;
        break;
      case GREATER:
      case NOT_EQUAL:
      case BETWEEN:
      case LESS:
      case REGEXP:
      default:
    }
    return true;
  }

  private boolean appendTrueFragment(TrueSegment fragment) {
    regexp = addWildcard(regexp);
    switch (operator) {
      case HEAD:
        operator = Operator.REGEXP;
        break;
      case EQUAL:
        fixStartAndStopRow();
        operator = Operator.BETWEEN;
        break;
      case GREATER:
      case NOT_EQUAL:
      case BETWEEN:
      case LESS:
      case REGEXP:
      default:
    }
    return false;
  }

  private void fixStartAndStopRow() {
    start = ByteUtils.copy(prefix);
    stop = incrementAsRange(prefix);
  }

  private byte[] addWildcard(byte[] value) {
    value = value == null ? new byte[0] : value;
    return endWith(value, RegexpMatchSegment.WILDCARD)
        ? value : ByteUtils.add(value, RegexpMatchSegment.WILDCARD);
  }

  private boolean endWith(byte[] value, byte[] suffix) {
    if (value.length < suffix.length) {
      return false;
    }
    for (int i = 1; i <= suffix.length; i++) {
      if (suffix[suffix.length - 1] != value[value.length - 1]) {
        return false;
      }
    }
    return true;
  }

  public static byte[] incrementAsRange(byte[] original) {
    byte[] next = ByteUtils.unsignedCopyAndIncrement(original);
    // in case of the length increased, the original value indicates the last.
    return next.length > original.length ? null : next;
  }

  @Override
  public String toString() {
    switch (operator) {
      case HEAD:
        return "";
      case EQUAL:
        return String.format("= %s", ByteUtils.toStringBinary(start));
      case NOT_EQUAL:
        return String.format("!= %s", ByteUtils.toStringBinary(start));
      case BETWEEN:
        return String.format("BETWEEN %s AND %s", ByteUtils.toStringBinary(start),
            ByteUtils.toStringBinary(stop));
      case GREATER:
        return String.format(">= %s", ByteUtils.toStringBinary(start));
      case LESS:
        return String.format("< %s", ByteUtils.toStringBinary(stop));
      case REGEXP:
        return String.format("LIKE %s", ByteUtils.toStringBinary(regexp));
      default:
        throw new IllegalArgumentException(operator.name());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FieldComparator)) {
      return false;
    }
    FieldComparator that = (FieldComparator) o;
    return operator == that.operator
        && Arrays.equals(prefix, that.prefix)
        && Arrays.equals(regexp, that.regexp)
        && Arrays.equals(start, that.start)
        && Arrays.equals(stop, that.stop);
  }

  @Override
  public int hashCode() {
    int result = operator.hashCode();
    result = 31 * result + Arrays.hashCode(prefix);
    result = 31 * result + Arrays.hashCode(regexp);
    result = 31 * result + Arrays.hashCode(start);
    result = 31 * result + Arrays.hashCode(stop);
    return result;
  }
}
