package jp.co.cyberagent.valor.sdk.formatter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
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

public class UrlEncodeFormatter extends AbstractAttributeValueFormatter {

  public static final String FORMATTER_NAME = "urlEncode";

  public UrlEncodeFormatter() {
  }

  public UrlEncodeFormatter(String attr) {
    super(attr);
  }

  @Override
  public Order getOrder() {
    return Order.NORMAL;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    String decodedVal = null;
    try {
      String strVal = new String(in, offset, length, StringAttributeType.CHARSET_NAME);
      decodedVal = URLDecoder.decode(strVal, StringAttributeType.CHARSET_NAME);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
    target.putAttribute(attrName, decodedVal);

    return length;
  }

  @Override
  protected byte[] serialize(Object attrVal, AttributeType<?> type) {
    if (attrVal == null) {
      throw new IllegalTypeException("string is expected but received null");
    }
    if (!(attrVal instanceof String)) {
      throw new IllegalTypeException("string is expected but " + attrVal.getClass());
    }
    String encodedVal = null;
    try {
      encodedVal = URLEncoder.encode((String) attrVal, StringAttributeType.CHARSET_NAME);
    } catch (UnsupportedEncodingException e) {
      throw new UnsupportedOperationException();
    }
    return ByteUtils.toBytes(encodedVal);
  }

  @Override
  protected FilterSegment convert(FilterSegment fragment) {
    if (fragment instanceof CompleteMatchSegment) {
      CompleteMatchSegment cmf = (CompleteMatchSegment) fragment;
      byte[] value = cmf.getValue();
      try {
        String strVal = ByteUtils.toString(value);
        String encodedVal = URLEncoder.encode(strVal, StringAttributeType.CHARSET_NAME);
        return cmf.copyWithNewValue(ByteUtils.toBytes(encodedVal));
      } catch (UnsupportedEncodingException e) {
        throw new UnsupportedOperationException();
      }
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
      return new UrlEncodeFormatter((String) attrName);
    }

    @Override
    public String getName() {
      return FORMATTER_NAME;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return UrlEncodeFormatter.class;
    }
  }
}
