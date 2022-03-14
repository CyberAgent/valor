package jp.co.cyberagent.valor.sdk.formatter;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.FalseSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.SingleValueFilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class DateTime2LongFormatter extends AbstractAttributeValueFormatter {

  private static final String FORMATTER_TYPE = "datetime2long";
  private static final String PATTERN_PROPKEY = "pattern";
  private static final String PATTERNS_PROPKEY = "patterns";
  private static final String ORDER_PROPKEY = "order";
  private static final String TIMEZONE_PROPKEY = "timezone";

  private static final long NANOSECONDS_PER_SECONDS = 1000000000L;

  private List<String> patterns;
  private ZoneId zoneId;
  private OrderType orderType;
  private DateTimeFormatter parseFormatter;
  private DateTimeFormatter printFormatter;


  public DateTime2LongFormatter() {
  }

  public DateTime2LongFormatter(Map<String, Object> config) {
    super((String) config.get(ATTRIBUTE_NAME_PROPKEY));
    setProperties(config);
  }

  @Override
  public Order getOrder() {
    return OrderType.ASC.equals(orderType) ? Order.NORMAL : Order.REVERSE;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    Long unixtime = ByteUtils.toLong(in, offset, length);
    if (orderType == OrderType.DESC) {
      unixtime = Long.MAX_VALUE - unixtime;
    }
    target.putAttribute(attrName, getDateTime(unixtime));
    return ByteUtils.SIZEOF_LONG;
  }

  @Override
  public byte[] serialize(Object attrVal, AttributeType type) {
    long unixtime = getUnixTime((String) attrVal);
    if (orderType == OrderType.DESC) {
      unixtime = Long.MAX_VALUE - unixtime;
    }
    return ByteUtils.toBytes(unixtime);
  }

  @Override
  protected FilterSegment convert(FilterSegment fragment) throws SerdeException {
    if (fragment instanceof TrueSegment || fragment instanceof FalseSegment) {
      return fragment;
    } else if (fragment instanceof SingleValueFilterSegment) {
      SingleValueFilterSegment svff = (SingleValueFilterSegment) fragment;
      byte[] value = svff.getValue();
      long unixtime = getUnixTime(new String(value));
      if (orderType == OrderType.DESC) {
        PredicativeExpression origin = fragment.getOrigin();
        byte[] ut = ByteUtils.toBytes(Long.MAX_VALUE - unixtime);
        if (svff instanceof GreaterThanSegment) {
          return new LessThanSegment(origin, ut, ((GreaterThanSegment) svff).isIncludeBorder());
        } else if (fragment instanceof LessThanSegment) {
          return new GreaterThanSegment(origin, ut, ((LessThanSegment) svff).isIncludeBorder());
        }
      } else {
        return svff.copyWithNewValue(ByteUtils.toBytes(unixtime));
      }
    }
    return TrueSegment.INSTANCE;
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(ATTRIBUTE_NAME_PROPKEY, attrName);
    props.put(PATTERNS_PROPKEY, patterns);
    props.put(ORDER_PROPKEY, orderType.getValue());
    props.put(TIMEZONE_PROPKEY, zoneId.getId());
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    if (props.containsKey(PATTERNS_PROPKEY)) {
      this.patterns = (List<String>) props.get(PATTERNS_PROPKEY);
    } else if (props.containsKey(PATTERN_PROPKEY)) {
      Object p = props.get(PATTERN_PROPKEY);
      if (p instanceof List) {
        this.patterns = (List<String>)p;
      } else {
        this.patterns = Arrays.asList((String)p);
      }
    } else {
      throw new IllegalArgumentException("pattern or patterns is needed");
    }
    DateTimeFormatterBuilder fmtBuilder = new DateTimeFormatterBuilder();
    for (int i = 0; i < patterns.size(); i++) {
      DateTimeFormatter fmt = DateTimeFormatter.ofPattern(patterns.get(i));
      fmtBuilder = fmtBuilder.optionalStart().append(fmt).optionalEnd();
      if (i == 0) {
        printFormatter = fmt;
      }
    }
    parseFormatter = fmtBuilder.toFormatter();

    zoneId = props.containsKey(TIMEZONE_PROPKEY) ? ZoneId.of((String) props.get(TIMEZONE_PROPKEY))
        : ZoneId.systemDefault();

    Object order = props.get(ORDER_PROPKEY);
    orderType = order == null ? OrderType.ASC : OrderType.getByValue(String.valueOf(order));
  }

  private long getUnixTime(String date) {
    TemporalAccessor parsed = parseFormatter.parse(date);
    ZoneId zoneId;
    try {
      zoneId = ZoneId.from(parsed);
    } catch (DateTimeException dte) {
      zoneId = this.zoneId;
    }
    return toEpochNanos(Instant.from(parseFormatter.withZone(zoneId).parse(date)));
  }

  private String getDateTime(Long unixtime) {
    Instant instant = ofEpochNanos(unixtime);
    return instant.atZone(zoneId).format(printFormatter);
  }

  private long toEpochNanos(Instant instant) {
    return instant.getEpochSecond() * NANOSECONDS_PER_SECONDS + instant.getNano();
  }

  private Instant ofEpochNanos(long unixtime) {
    long secondsPart = unixtime / NANOSECONDS_PER_SECONDS;
    long nanoSecondsPart = unixtime % NANOSECONDS_PER_SECONDS;
    return Instant.ofEpochSecond(secondsPart, nanoSecondsPart);
  }

  public static class Factory implements FormatterFactory {

    @Override
    public Formatter create(Map config) {
      Object attrName = config.get(ATTRIBUTE_NAME_PROPKEY);
      if (attrName == null) {
        throw new IllegalArgumentException(ATTRIBUTE_NAME_PROPKEY + " is not set");
      }
      if (!config.containsKey(PATTERN_PROPKEY) && !config.containsKey(PATTERNS_PROPKEY)) {
        throw new IllegalArgumentException("pattern or patterns is needed");
      }

      return new DateTime2LongFormatter(config);
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return DateTime2LongFormatter.class;
    }
  }

  private enum OrderType {
    ASC("asc"),
    DESC("desc");

    private final String value;

    OrderType(final String value) {
      this.value = value;
    }

    public static OrderType getByValue(String order) {
      switch (order) {
        case ("asc"):
          return ASC;
        case ("desc"):
          return DESC;
        default:
          throw new SerdeException("unknown order type " + order);
      }
    }

    public String getValue() {
      return this.value;
    }
  }
}
