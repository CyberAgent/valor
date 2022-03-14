package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;

/**
 * a schema element representing a value of a single attribute
 */
public class AttributeValueFormatter extends AbstractAttributeValueFormatter {
  public static final String FORMATTER_TYPE = "attr";

  public static final String ATTRIBUTE_NAME_PROPKEY = "attr";

  public AttributeValueFormatter() {
  }

  public AttributeValueFormatter(String attrName) {
    super(attrName);
  }

  public static AttributeValueFormatter create(String attrName) {
    AttributeValueFormatter elm = new AttributeValueFormatter();
    elm.attrName = attrName;
    return elm;
  }

  @Override
  protected byte[] serialize(Object attrVal, AttributeType<?> type) {
    return type.serialize(attrVal);
  }

  @Override
  public Order getOrder() {
    return Order.NORMAL;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, final TupleDeserializer target)
      throws SerdeException {
    target.putAttribute(attrName, in, offset, length);
    return length;
  }

  @Override
  protected FilterSegment convert(FilterSegment fragment) {
    return fragment;
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AttributeValueFormatter)) {
      return false;
    }
    return Objects.equals(attrName, ((AttributeValueFormatter) obj).attrName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(attrName);
  }

  public static class Factory implements FormatterFactory {
    @Override
    public Formatter create(Map config) {
      Object attrName = config.get(ATTRIBUTE_NAME_PROPKEY);
      if (attrName == null) {
        throw new IllegalArgumentException(ATTRIBUTE_NAME_PROPKEY + " is not set");
      }
      return create((String) attrName);
    }

    public AttributeValueFormatter create(String attrName) {
      AttributeValueFormatter elm = new AttributeValueFormatter();
      elm.attrName = attrName;
      return elm;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return AttributeValueFormatter.class;
    }
  }
}
