package jp.co.cyberagent.valor.sdk.formatter;

import java.io.IOException;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.exception.MismatchByteArrayException;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class Number2StringFormatter extends AbstractAttributeValueFormatter {

  public static final String FORMATTER_NAME = "number2string";

  public Number2StringFormatter() {
  }

  public Number2StringFormatter(String attr) {
    super(attr);
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    String strVal = null;
    try {
      strVal = new String(in, offset, length, StringAttributeType.CHARSET_NAME);
    } catch (IOException e) {
      throw new SerdeException(e);
    }

    try {
      if (strVal.indexOf(".") < 0) {
        Long longVal = Long.parseLong(strVal);
        target.putAttribute(attrName, longVal);
      } else {
        Float floatVal = Float.parseFloat(strVal);
        target.putAttribute(attrName, floatVal);
      }
    } catch (NumberFormatException nfe) {
      throw new MismatchByteArrayException(strVal + " is not a number expression", nfe);
    }
    return length;
  }

  @Override
  protected byte[] serialize(Object attrVal, AttributeType<?> type) {
    if (attrVal == null) {
      throw new IllegalTypeException("long or float is expected but received null");
    }
    if (attrVal instanceof Long) {
      return ByteUtils.toBytes(Long.toString((Long) attrVal));
    } else if (attrVal instanceof Float) {
      return ByteUtils.toBytes(Float.toString((Float) attrVal));
    } else {
      throw new IllegalTypeException("long of float is expected but " + attrVal.getClass());
    }
  }

  @Override
  protected FilterSegment convert(FilterSegment fragment) {
    if (fragment instanceof CompleteMatchSegment) {
      CompleteMatchSegment cmf = (CompleteMatchSegment) fragment;
      byte[] value = cmf.getValue();
      if (value.length == 8) {
        return cmf.copyWithNewValue(ByteUtils.toBytes(Long.toString(ByteUtils.toLong(value))));
      } else if (value.length == 4) {
        return cmf.copyWithNewValue(ByteUtils.toBytes(Float.toString(ByteUtils.toFloat(value))));
      }
      return cmf;
    }
    return TrueSegment.INSTANCE;
  }

  @Override
  public String getName() {
    return FORMATTER_NAME;
  }

  public static class Factory implements FormatterFactory {

    @Override
    public Formatter create(Map config) {
      Object attrName = config.get(ATTRIBUTE_NAME_PROPKEY);
      if (attrName == null) {
        throw new IllegalArgumentException(ATTRIBUTE_NAME_PROPKEY + " is not set");
      }
      return new Number2StringFormatter((String) attrName);
    }

    @Override
    public String getName() {
      return FORMATTER_NAME;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return Number2StringFormatter.class;
    }
  }
}
