package jp.co.cyberagent.valor.sdk.formatter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.BetweenSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.FalseSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.RegexpMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.SingleValueFilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class String2DateTimeFrameFormatter extends AbstractAttributeValueFormatter {

  public static final int BYTE_LENGTH = 8;

  public static final String FORMATTER_TYPE = "string2datetimeframe";

  public String2DateTimeFrameFormatter() {
  }

  public String2DateTimeFrameFormatter(String attrName) {
    super(attrName);
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    if (length != BYTE_LENGTH) {
      throw new SerdeException("invalid length: 8 is expected buf " + length);
    }
    DateTimeFrame dtr = null;
    try {
      dtr = DateTimeFrame.parse(in, offset);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
    target.putAttribute(attrName, dtr.toString());
    return BYTE_LENGTH;
  }

  // VisibleForTesting
  @Override
  public byte[] serialize(Object attrVal, AttributeType type) {
    String v = (String) attrVal;
    DateTimeFrame dtf = DateTimeFrame.parse(v);
    return dtf.toBytes();
  }

  @Override
  protected FilterSegment convert(FilterSegment fragment) throws SerdeException {
    if (fragment instanceof TrueSegment || fragment instanceof FalseSegment) {
      return fragment;
    } else if (fragment instanceof SingleValueFilterSegment
        && !(fragment instanceof RegexpMatchSegment)) {
      SingleValueFilterSegment svff = (SingleValueFilterSegment) fragment;
      return svff.copyWithNewValue(toDateTimeFrameBytes(svff.getValue()));
    } else if (fragment instanceof BetweenSegment) {
      BetweenSegment bf = (BetweenSegment) fragment;
      return new BetweenSegment(bf.getOrigin(),
          bf.isIncludeMin(), toDateTimeFrameBytes(bf.getMin()),
          bf.isIncludeMax(), toDateTimeFrameBytes(bf.getMax()));
    }
    return TrueSegment.INSTANCE;
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  private byte[] toDateTimeFrameBytes(byte[] value) throws SerdeException {
    String strVal = ByteUtils.toString(value);
    DateTimeFrame dtf = DateTimeFrame.parse(strVal);
    return dtf.toBytes();
  }

  public enum FrameType {
    YEARLY((byte) 1), MONTHLY((byte) 2), DAILY((byte) 3), HOURLY((byte) 4), MINUTELY((byte) 5),
    SECONDLY((byte) 6);

    private byte value;

    FrameType(byte f) {
      this.value = f;
    }

    public static FrameType getByValue(byte value) throws SerdeException {
      switch (value) {
        case (1):
          return YEARLY;
        case (2):
          return MONTHLY;
        case (3):
          return DAILY;
        case (4):
          return HOURLY;
        case (5):
          return MINUTELY;
        case (6):
          return SECONDLY;
        default:
          throw new SerdeException("unknown frame type " + value);
      }
    }

    public byte value() {
      return this.value;
    }

    public byte getValue() {
      return value;
    }
  }

  public static class Factory implements FormatterFactory {

    @Override
    public Formatter create(Map config) {
      Object attrName = config.get(ATTRIBUTE_NAME_PROPKEY);
      if (attrName == null) {
        throw new IllegalArgumentException(ATTRIBUTE_NAME_PROPKEY + " is not set (" + config + ")");
      }
      return create((String) attrName);
    }

    public String2DateTimeFrameFormatter create(String attrName) {
      String2DateTimeFrameFormatter elm = new String2DateTimeFrameFormatter();
      elm.attrName = attrName;
      return elm;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return String2DateTimeFrameFormatter.class;
    }
  }

  static class DateTimeFrame {

    // CHECKSTYLE:OFF
    private static Pattern format = Pattern.compile(
        "([\\d]{4})(?:-([\\d]{2})(?:-([\\d]{2})(?:\\s([\\d]{2})(?:\\:([\\d]{2})(?:\\:([\\d]{2}))?)?)?)?)?");
    // CHECKSTYLE:ON

    private FrameType frameType;
    private Short year;
    private Byte month;
    private Byte day;
    private Byte hour;
    private Byte minute;
    private Byte second;

    DateTimeFrame(FrameType frameType, Integer year, Integer month, Integer day, Integer hour,
                  Integer minute, Integer second) {
      this(frameType, year == null ? -1 : year.shortValue(), month == null ? -1 :
          month.byteValue(), day == null ? -1 : day.byteValue(), hour == null ? -1 :
          hour.byteValue(), minute == null ? -1 : minute.byteValue(), second == null ? -1 :
          second.byteValue());
    }

    DateTimeFrame(FrameType frameType, Short year, Byte month, Byte day, Byte hour, Byte minute,
                  Byte second) {
      this.frameType = frameType;
      this.year = year == null ? -1 : year;
      this.month = month == null ? -1 : month;
      this.day = day == null ? -1 : day;
      this.hour = hour == null ? -1 : hour;
      this.minute = minute == null ? -1 : minute;
      this.second = second == null ? -1 : second;
    }

    public static DateTimeFrame parse(String exp) {
      Matcher matcher = format.matcher(exp);
      List<Integer> dateTimeElements = new ArrayList<>();
      matcher.find();
      for (int i = 1; i <= matcher.groupCount(); i++) {
        String dateTimeElement = matcher.group(i);
        if (dateTimeElement == null) {
          break;
        }
        dateTimeElements.add(Integer.parseInt(dateTimeElement));
      }
      return DateTimeFrame.newInstance(dateTimeElements.toArray(new Integer[0]));
    }

    public static DateTimeFrame parse(byte[] in, int offset) throws IOException {
      FrameType type = FrameType.getByValue(in[offset]);
      short year = ByteUtils.toShort(in, offset + 1);
      byte month = in[offset + 1 + ByteUtils.SIZEOF_SHORT];
      byte day = in[offset + 2 + ByteUtils.SIZEOF_SHORT];
      byte hour = in[offset + 3 + ByteUtils.SIZEOF_SHORT];
      byte minute = in[offset + 4 + ByteUtils.SIZEOF_SHORT];
      byte second = in[offset + 5 + ByteUtils.SIZEOF_SHORT];
      return new DateTimeFrame(type, year, month, day, hour, minute, second);
    }

    public static DateTimeFrame newInstance(Integer... date) {
      if (date == null) {
        throw new IllegalArgumentException(Arrays.toString(date));
      }
      for (int i = 0; i < date.length; i++) {
        if (date[i] < 0) {
          throw new IllegalArgumentException(String.format("%d th argument is negative (%d) ", i,
              date[i]));
        }
      }
      switch (date.length) {
        case 1:
          return new DateTimeFrame(FrameType.YEARLY, date[0], null, null, null, null, null);
        case 2:
          return new DateTimeFrame(FrameType.MONTHLY, date[0], date[1], null, null, null, null);
        case 3:
          return new DateTimeFrame(FrameType.DAILY, date[0], date[1], date[2], null, null, null);
        case 4:
          return new DateTimeFrame(FrameType.HOURLY, date[0], date[1], date[2], date[3], null,
              null);
        case 5:
          return new DateTimeFrame(FrameType.MINUTELY, date[0], date[1], date[2], date[3],
              date[4], null);
        case 6:
          return new DateTimeFrame(FrameType.SECONDLY, date[0], date[1], date[2], date[3],
              date[4], date[5]);
        default:
          throw new IllegalArgumentException(Arrays.toString(date));
      }
    }

    public byte[] toBytes() {
      ByteBuffer buf = ByteBuffer.allocate(BYTE_LENGTH);
      buf.put(frameType.value());
      buf.putShort(year);
      buf.put(month);
      buf.put(day);
      buf.put(hour);
      buf.put(minute);
      buf.put(second);
      return buf.array();
    }

    @Override
    public String toString() {
      StringBuilder buf = new StringBuilder();
      buf.append(String.format("%04d", this.year));
      if (FrameType.YEARLY.equals(this.frameType)) {
        return buf.toString();
      }
      buf.append(String.format("-%02d", this.month));
      if (FrameType.MONTHLY.equals(this.frameType)) {
        return buf.toString();
      }
      buf.append(String.format("-%02d", this.day));
      if (FrameType.DAILY.equals(this.frameType)) {
        return buf.toString();
      }
      buf.append(String.format(" %02d", this.hour));
      if (FrameType.HOURLY.equals(this.frameType)) {
        return buf.toString();
      }
      buf.append(String.format(":%02d", this.minute));
      if (FrameType.MINUTELY.equals(this.frameType)) {
        return buf.toString();
      }
      buf.append(String.format(":%02d", this.second));
      return buf.toString();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((frameType == null) ? 0 : frameType.hashCode());
      result = prime * result + ((year == null) ? 0 : year.hashCode());
      result = prime * result + ((month == null) ? 0 : month.hashCode());
      result = prime * result + ((day == null) ? 0 : day.hashCode());
      result = prime * result + ((hour == null) ? 0 : hour.hashCode());
      result = prime * result + ((minute == null) ? 0 : minute.hashCode());
      result = prime * result + ((second == null) ? 0 : second.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      DateTimeFrame other = (DateTimeFrame) obj;

      if (frameType != other.frameType) {
        return false;
      }
      if (year == null) {
        if (other.year != null) {
          return false;
        }
      } else if (!year.equals(other.year)) {
        return false;
      }
      if (month == null) {
        if (other.month != null) {
          return false;
        }
      } else if (!month.equals(other.month)) {
        return false;
      }
      if (day == null) {
        if (other.day != null) {
          return false;
        }
      } else if (!day.equals(other.day)) {
        return false;
      }
      if (hour == null) {
        if (other.hour != null) {
          return false;
        }
      } else if (!hour.equals(other.hour)) {
        return false;
      }
      if (minute == null) {
        if (other.minute != null) {
          return false;
        }
      } else if (!minute.equals(other.minute)) {
        return false;
      }
      if (second == null) {
        if (other.second != null) {
          return false;
        }
      } else if (!second.equals(other.second)) {
        return false;
      }
      return true;
    }
  }
}
