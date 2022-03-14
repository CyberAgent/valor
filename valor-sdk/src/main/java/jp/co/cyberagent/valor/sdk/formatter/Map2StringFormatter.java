package jp.co.cyberagent.valor.sdk.formatter;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.spi.exception.MismatchByteArrayException;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class Map2StringFormatter extends AbstractAttributeValueFormatter {

  public static final String FORMATTER_TYPE = "map2string";

  public static final String DEFAULT_EQUAL_SEPARATOR = "=";

  public static final String DEFAULT_ITEM_SEPARATOR = "\t";

  private String equalSeparator = DEFAULT_EQUAL_SEPARATOR;

  private String itemSeparator = DEFAULT_ITEM_SEPARATOR;

  public Map2StringFormatter(String attr) {
    super(attr);
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    if (length == 0) {
      target.putAttribute(attrName, new HashMap<>());
      return length;
    }
    String strVal;
    try {
      strVal = new String(in, offset, length, StringAttributeType.CHARSET_NAME);
    } catch (IOException e) {
      throw new SerdeException(e);
    }

    String[] items = strVal.split(this.itemSeparator);
    Map m = new HashMap();
    for (String item : items) {
      int equalIndex = item.indexOf(this.equalSeparator);
      if (equalIndex < 0) {
        throw new MismatchByteArrayException(
            item + " is not formatted by " + this.getClass().getCanonicalName());
      }
      String attrName = item.substring(0, equalIndex);
      String attrVal = item.substring(equalIndex + 1);
      m.put(attrName, attrVal);
    }
    target.putAttribute(attrName, m);
    return length;
  }

  @Override
  protected byte[] serialize(Object attrVal, AttributeType<?> type) {
    String strVal = buildStringValue((Map<String, String>) attrVal);
    return ByteUtils.toBytes(strVal);
  }

  protected String buildStringValue(Map<String, String> m) {
    SortedSet<String> keys = new TreeSet<>(m.keySet());
    StringJoiner joiner = new StringJoiner(itemSeparator);
    for (String key : keys) {
      joiner.add(String.format("%s%s%s", key, this.equalSeparator, m.get(key)));
    }
    return joiner.toString();
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction) {
    for (PrimitivePredicate p : conjunction) {
      if (!(p instanceof EqualOperator)) {
        continue;
      }

      EqualOperator eo = (EqualOperator) p;
      AttributeNameExpression attr = null;
      ConstantExpression value = null;

      if (eo.getLeft() instanceof AttributeNameExpression && eo
          .getRight() instanceof ConstantExpression) {
        attr = (AttributeNameExpression) eo.getLeft();
        value = (ConstantExpression) eo.getRight();
      } else if (eo.getRight() instanceof AttributeNameExpression && eo
          .getLeft() instanceof ConstantExpression) {
        attr = (AttributeNameExpression) eo.getRight();
        value = (ConstantExpression) eo.getLeft();
      } else {
        continue;
      }
      if (!attrName.equals(attr.getName())) {
        continue;
      }
      Map<String, String> m = (Map) value.getValue();
      String str = buildStringValue(m);
      serializer.write(null, new CompleteMatchSegment(p, ByteUtils.toBytes(str)));
      return;
    }
    serializer.write(null, TrueSegment.INSTANCE);
  }

  @Override
  protected FilterSegment convert(FilterSegment fragment) {
    throw new UnsupportedOperationException(
        "should not be invoked because of outer method is overridden");
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
      return new Map2StringFormatter((String) attrName);
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return Map2StringFormatter.class;
    }
  }
}
