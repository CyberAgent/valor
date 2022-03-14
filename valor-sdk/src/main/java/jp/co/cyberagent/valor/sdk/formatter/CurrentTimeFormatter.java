package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class CurrentTimeFormatter extends AbstractAttributeValueFormatter {

  public static final String FORMATTER_TYPE = "currentTime";

  public CurrentTimeFormatter(String attr) {
    super(attr);
  }

  @Override
  protected byte[] serialize(Object attrVal, AttributeType<?> type) {
    return ByteUtils.toBytes(getCurrentTime());
  }

  @Override
  protected FilterSegment convert(FilterSegment fragment) {
    return fragment;
  }

  @Override
  public Order getOrder() {
    return Order.NORMAL;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    final Long v = ByteUtils.toLong(in, offset);
    target.putAttribute(attrName, v);
    return LongAttributeType.SIZE;
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  // @VisibleForTesting
  long getCurrentTime() {
    return System.currentTimeMillis();
  }

  public static class Factory implements FormatterFactory {

    @Override
    public Formatter create(Map config) {
      final Object attrName = config.get(ATTRIBUTE_NAME_PROPKEY);
      if (attrName == null) {
        throw new IllegalArgumentException(ATTRIBUTE_NAME_PROPKEY + " is not set");
      }
      return new CurrentTimeFormatter((String) attrName);
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return CurrentTimeFormatter.class;
    }
  }
}
