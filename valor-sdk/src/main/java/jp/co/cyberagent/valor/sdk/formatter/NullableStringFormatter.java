package jp.co.cyberagent.valor.sdk.formatter;

import java.io.UnsupportedEncodingException;
import java.util.Map;
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

public class NullableStringFormatter extends AbstractAttributeValueFormatter {

  public static final String FORMATTER_TYPE = "nullableString";
  private static final byte NUL = 0x00;
  private static final byte[] NUL_ARRAY = ByteUtils.toBytes(NUL);

  public NullableStringFormatter() {
  }

  public NullableStringFormatter(String attrName) {
    super(attrName);
  }

  @Override
  public Order getOrder() {
    return Order.NORMAL;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    String value = null;
    // if buf is {0, 0}, skip setting the value. This is equivalent to set null.
    if (!ByteUtils.equals(in, offset, length, NUL_ARRAY, 0, NUL_ARRAY.length)) {
      try {
        value = new String(in, offset, length, StringAttributeType.CHARSET_NAME);
      } catch (UnsupportedEncodingException e) {
        throw new SerdeException(e);
      }
      target.putAttribute(attrName, value);
    }

    return length;
  }

  @Override
  protected byte[] serialize(Object attrVal, AttributeType type) {
    if (attrVal == null) {
      return ByteUtils.toBytes(NUL);
    }

    String value = String.valueOf(attrVal);
    return ByteUtils.toBytes(value);
  }

  @Override
  protected FilterSegment convert(FilterSegment fragment) {
    if (fragment instanceof CompleteMatchSegment) {
      CompleteMatchSegment cmf = (CompleteMatchSegment) fragment;
      byte[] value = cmf.getValue();
      return value == null ? cmf.copyWithNewValue(ByteUtils.toBytes(NUL)) : cmf;
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
      Object attrName = config.get(ATTRIBUTE_NAME_PROPKEY);
      if (attrName == null) {
        throw new IllegalArgumentException(ATTRIBUTE_NAME_PROPKEY + " is not set");
      }
      return new NullableStringFormatter((String) attrName);
    }


    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return NullableStringFormatter.class;
    }
  }
}
