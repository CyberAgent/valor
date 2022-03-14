package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class RegexpMatchSegment extends SingleValueFilterSegment {

  public static final byte[] WILDCARD = ".*".getBytes();
  private final String prefix;
  private final boolean positive;
  private final Predicate<byte[]> predicate;
  private final Pattern fixPrefix = Pattern.compile("^([^\\.\\*\\%\\_]+).*");

  public RegexpMatchSegment(PredicativeExpression origin, byte[] value, boolean positive) {
    super(origin, value);
    if (positive) {
      Matcher matcher = fixPrefix.matcher(ByteUtils.toString(value));
      this.prefix = matcher.matches() ? matcher.group(1) : null;
    } else {
      this.prefix = null;
    }
    this.positive = positive;
    Pattern pattern = Pattern.compile(ByteUtils.toString(value));
    predicate = positive
        ? (s) -> pattern.matcher(ByteUtils.toString(value)).matches()
        : (s) -> !pattern.matcher(ByteUtils.toString(value)).matches();
  }

  public String getPrefix() {
    return this.prefix;
  }

  @Override
  public Type type() {
    return Type.REGEXP;
  }

  @Override
  public FilterSegment mergeByAnd(FilterSegment otherFragment) {
    return otherFragment;
  }

  @Override
  public boolean evaluate(byte[] value) {
    return predicate.test(value);
  }

  public boolean isPositive() {
    return positive;
  }

  @Override
  public SingleValueFilterSegment copyWithNewValue(byte[] value) {
    return new RegexpMatchSegment(origin, value, positive);
  }
}
