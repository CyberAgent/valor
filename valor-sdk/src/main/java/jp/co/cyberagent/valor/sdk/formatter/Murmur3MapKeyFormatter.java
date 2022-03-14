package jp.co.cyberagent.valor.sdk.formatter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.RegexpOperator;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfMapKeys;
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
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import jp.co.cyberagent.valor.spi.util.MurmurHash3;

/**
 * set murmur3_32 hash (int) of map keys
 */
public class Murmur3MapKeyFormatter extends Formatter {

  public static final String FORMATTER_TYPE = "murmur3mapkey";

  public static final String MAP_ATTR_PROPKEY = "mapAttr";

  public static final String LENGTH_PROPKEY = "length";

  @Deprecated
  public static Murmur3MapKeyFormatter create(String mapAttr) {
    Murmur3MapKeyFormatter formatter = new Murmur3MapKeyFormatter();
    formatter.setProperties(new HashMap<String, Object>() {
      {
        put(MAP_ATTR_PROPKEY, mapAttr);
      }
    });
    return formatter;
  }

  protected int length;

  protected String mapAttr;


  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    return this.length;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    MapAttributeType type = (MapAttributeType) tuple.getAttributeType(mapAttr);
    Map m = (Map) tuple.getAttribute(mapAttr);
    byte[] fragmentValue = toHashBytes(m.keySet(), type.getKeyType(), length);
    serializer.write(null, fragmentValue);
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws SerdeException {
    for (PrimitivePredicate cond : conjunction) {
      if (cond instanceof EqualOperator || cond instanceof RegexpOperator) {
        byte[] fragmentValues = extractAndSerialize((BinaryPrimitivePredicate) cond);
        if (fragmentValues != null) {
          serializer.write(null, new CompleteMatchSegment(null, fragmentValues));
          return;
        }
      }
    }
  }

  private byte[] extractAndSerialize(BinaryPrimitivePredicate eq) {
    Optional<AttributeNameExpression> operandAttr = eq.getAttributeExpIfUnaryPredicate();
    if (operandAttr.isPresent() && mapAttr.equals(operandAttr.get().getName())) {
      Map m = (Map) eq.getAttributeAndConstantIfExists().getSecond();
      MapAttributeType type = (MapAttributeType) operandAttr.get().getType();
      return toHashBytes(m.keySet(), type.getKeyType(), length);
    }
    byte[] serializedValue = toHashBytes(eq.getLeft(), eq.getRight());
    if (serializedValue != null) {
      return serializedValue;
    }
    return toHashBytes(eq.getRight(), eq.getLeft());
  }


  private byte[] toHashBytes(Expression exp1, Expression exp2) {
    if (!(exp1 instanceof UdfExpression)) {
      return null;
    }
    Udf udf = ((UdfExpression) exp1).getFunction();
    if (!(udf instanceof UdfMapKeys)) {
      return null;
    }
    Expression arg = ((UdfExpression) exp1).getArguments().get(0);
    if (!(arg instanceof AttributeNameExpression)) {
      return null;
    }
    if (!mapAttr.equals(((AttributeNameExpression) arg).getName())) {
      return null;
    }
    Collection keys = null;
    if (exp2 instanceof ConstantExpression) {
      Object v = ((ConstantExpression<?>) exp2).getValue();
      if (v instanceof Collection) {
        keys = (Collection) v;
      } else {
        return null;
      }
    }
    return toHashBytes(keys, ((MapAttributeType)arg.getType()).getKeyType(), length);
  }

  public static byte[] toHashBytes(Collection keys, AttributeType keyType, int length) {
    SortedSet sortedKeys = new TreeSet(keys);
    List<byte[]> fragmentValues = new ArrayList(sortedKeys.size());
    int totalLength = 0;
    for (Object key: sortedKeys) {
      byte[] value = keyType.serialize(key);
      fragmentValues.add(value);
      totalLength += value.length;
    }
    ByteBuffer buf = ByteBuffer.allocate(totalLength);
    fragmentValues.stream().forEach(buf::put);
    byte[] h = calculateHash(buf.array());
    return length == h.length ? h : Arrays.copyOf(h, length);
  }

  private static byte[] calculateHash(byte[] value) {
    return ByteUtils.toBytes(MurmurHash3.hash(value));
  }


  @Override
  public boolean containsAttribute(String attr) {
    return false;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(LENGTH_PROPKEY, length);
    props.put(MAP_ATTR_PROPKEY, mapAttr);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    if (props.containsKey(LENGTH_PROPKEY)) {
      length = (int) props.get(LENGTH_PROPKEY);
      if (length < 1 || length > getMaxLength()) {
        throw new IllegalArgumentException(LENGTH_PROPKEY + " is out of range");
      }
    } else {
      length = getMaxLength();
    }
    Object attr = props.get(MAP_ATTR_PROPKEY);
    if (attr == null || !(attr instanceof String)) {
      throw new IllegalArgumentException(
          attr == null ? "attr is not specified" : attr + " is not a string");
    }
    this.mapAttr = (String) attr;
  }

  public int getMaxLength() {
    return ByteUtils.SIZEOF_INT;
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  public static class Factory implements FormatterFactory {

    public Formatter create(int length, String attrName) {
      return create(new HashMap<String, Object>() {
        {
          put(LENGTH_PROPKEY, length);
          put(MAP_ATTR_PROPKEY, attrName);
        }
      });
    }

    @Override
    public Formatter create(Map config) {
      Murmur3MapKeyFormatter formatter = new Murmur3MapKeyFormatter();
      formatter.setProperties(config);
      return formatter;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return Murmur3MapKeyFormatter.class;
    }
  }
}
