package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.exception.MismatchByteArrayException;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.FloatAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class String2NumberFormatter extends AbstractAttributeValueFormatter {

  public static final String FORMATTER_TYPE = "string2number";

  public String2NumberFormatter() {
  }

  public String2NumberFormatter(String attrName) {
    super(attrName);
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    String value;
    if (length == LongAttributeType.SIZE) {
      value = Long.toString(ByteUtils.toLong(in, offset));
    } else if (length == FloatAttributeType.SIZE) {
      value = Float.toString(ByteUtils.toFloat(in, offset));
    } else {
      throw new IllegalTypeException("long or float is expected but the received bytes size is "
          + length);
    }
    target.putAttribute(attrName, value);
    return length;
  }

  @Override
  protected byte[] serialize(Object attrVal, AttributeType type) {
    String value = (String) attrVal;
    // TODO push up null support check to a super method (in the scope of 9132)
    if (value == null) {
      throw new IllegalTypeException("string is expected but received null");
    }
    try {
      if (value.contains(".")) {
        Float floatVal = Float.parseFloat(value);
        return ByteUtils.toBytes(floatVal);
      } else {
        Long longVal = Long.parseLong(value);
        return ByteUtils.toBytes(longVal);
      }
    } catch (NumberFormatException nfe) {
      throw new MismatchByteArrayException(value + " is not a number expression", nfe);
    }
  }

  @Override
  protected FilterSegment convert(FilterSegment fragment) {
    if (fragment instanceof CompleteMatchSegment) {
      CompleteMatchSegment cmf = (CompleteMatchSegment) fragment;
      byte[] value = cmf.getValue();
      if (value.length == LongAttributeType.SIZE) {
        return cmf.copyWithNewValue(ByteUtils.toBytes(Long.toString(ByteUtils.toLong(value))));
      } else if (value.length == FloatAttributeType.SIZE) {
        return cmf.copyWithNewValue(ByteUtils.toBytes(Float.toString(ByteUtils.toFloat(value))));
      }
      return cmf;
    }
    return TrueSegment.INSTANCE;
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  public static class Factory implements FormatterFactory {

    @Override
    public Formatter create(Map config) {
      return new String2NumberFormatter();
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return String2NumberFormatter.class;
    }
  }
}
