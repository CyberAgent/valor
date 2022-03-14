package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfMapValue;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.Udf;
import jp.co.cyberagent.valor.spi.plan.model.UdfExpression;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;

public class MapEntryFormatter extends Formatter {

  public static final String FORMATTER_TYPE = "mapEntry";

  public static final String ATTRIBUTE_NAME_PROPKEY = "attr";

  public static final String MAP_KEY_PROPKEY = "key";

  private String attr;

  private String key;

  public MapEntryFormatter() {
  }

  public static MapEntryFormatter create(String attrName, String key) {
    MapEntryFormatter elm = new MapEntryFormatter();
    elm.attr = attrName;
    elm.key = key;
    return elm;
  }


  @Override
  public Order getOrder() {
    return Order.NORMAL;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    MapAttributeType mapType = (MapAttributeType) target.getRelation().getAttributeType(attr);
    AttributeType type = mapType.getValueType();
    Object val = type.read(in, offset, length);
    Map m = (Map) target.getAttribute(attr);
    if (m == null) {
      m = new HashMap();
    }
    m.put(key, val);
    target.putAttribute(attr, m);
    return length;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    MapAttributeType mapType = (MapAttributeType) tuple.getAttributeType(attr);
    AttributeType type = mapType.getValueType();
    Map m = (Map) tuple.getAttribute(attr);
    Object v = m.get(key);
    byte[] valueAsBytes = type.serialize(v);
    serializer.write(null, valueAsBytes);
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws ValorException {
    FilterSegment segment = TrueSegment.INSTANCE;
    for (PrimitivePredicate predicate : conjunction) {
      FilterSegment tmpSegment = toFilterSegment(predicate);
      if (tmpSegment != null) {
        segment =  segment.mergeByAnd(tmpSegment);
      }
    }
    serializer.write(null, segment);
  }

  private FilterSegment toFilterSegment(PrimitivePredicate predicate) {
    if (predicate instanceof BinaryPrimitivePredicate) {
      Expression left = ((BinaryPrimitivePredicate) predicate).getLeft();
      Expression right = ((BinaryPrimitivePredicate) predicate).getRight();
      AttributeType valueType = extractValueType(left);
      if (valueType != null) {
        if (right instanceof ConstantExpression) {
          byte[] v = valueType.serialize(((ConstantExpression<?>) right).getValue());
          return toFilterSegment((BinaryPrimitivePredicate) predicate, v);
        }
      } else if (left instanceof ConstantExpression) {
        valueType = extractValueType(left);
        if (valueType != null) {
          byte[] v = valueType.serialize(((ConstantExpression<?>) left).getValue());
          return toFilterSegment((BinaryPrimitivePredicate) predicate, v);
        }
      }
    }
    return null;
  }

  private FilterSegment toFilterSegment(BinaryPrimitivePredicate predicate, byte[] v) {
    if (predicate instanceof EqualOperator) {
      return new CompleteMatchSegment(predicate, v);
    }
    return null;
  }

  private AttributeType extractValueType(Expression e) {
    if (!(e instanceof UdfExpression)) {
      return null;
    }
    Udf udf = ((UdfExpression) e).getFunction();
    if (!(udf instanceof UdfMapValue)) {
      return null;
    }
    List<Expression> args = ((UdfExpression) e).getArguments();
    Expression mapAttr = args.get(0);
    if (!(mapAttr instanceof AttributeNameExpression)) {
      return null;
    }
    Expression mapKey = args.get(1);
    if (!(mapAttr instanceof ConstantExpression)) {
      return null;
    }
    if (attr.equals(((AttributeNameExpression) mapAttr).getName())
        && key.equals(((ConstantExpression) mapKey).getValue())) {
      MapAttributeType mapType = (MapAttributeType) mapAttr.getType();
      return mapType.getValueType();
    }
    return null;
  }

  @Override
  public boolean containsAttribute(String attr) {
    return false;
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(ATTRIBUTE_NAME_PROPKEY, attr);
    props.put(MAP_KEY_PROPKEY, key);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    this.attr = (String) props.get(ATTRIBUTE_NAME_PROPKEY);
    this.key = (String) props.get(MAP_KEY_PROPKEY);
  }

  public static class Factory implements FormatterFactory {
    @Override
    public Formatter create(Map config) {
      Object attrName = config.get(ATTRIBUTE_NAME_PROPKEY);
      if (attrName == null) {
        throw new IllegalArgumentException(ATTRIBUTE_NAME_PROPKEY + " is not set");
      }
      Object key = config.get(MAP_KEY_PROPKEY);
      if (key == null) {
        throw new IllegalArgumentException(MAP_KEY_PROPKEY + " is not set");
      }

      return create((String) attrName, (String) key);
    }

    public MapEntryFormatter create(String attrName, String key) {
      MapEntryFormatter elm = new MapEntryFormatter();
      elm.attr = attrName;
      elm.key = key;
      return elm;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return MapEntryFormatter.class;
    }
  }
}
