package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.serde.ContinuousRecordsDeserializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeNode;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializerWrapper;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

/**
 * must follow AttributeNameSchemaELemement.
 */
public class MapValueFormatter extends DisassembleFormatter {

  public static final String FORMATTER_TYPE = "mapValue";

  public static final String MAP_ATTR_PROPKEY = "mapAttr";

  public static final String CONVERTER_PROPKEY = "converter";

  protected String mapAttr;

  @Deprecated
  public static MapValueFormatter create(String mapAttr) {
    MapValueFormatter e = new MapValueFormatter();
    e.mapAttr = mapAttr;
    return e;
  }

  protected MapValueFormatter() {
  }

  public MapValueFormatter(String mapAttr) {
    this.mapAttr = mapAttr;
  }


  @Override
  public Order getOrder() {
    // TODO change to Order.NORMAL after converter is removed
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer tuple)
      throws SerdeException {
    // TODO check in configuration
    AttributeType type = tuple.getRelation().getAttributeType(mapAttr);
    if (!(type instanceof MapAttributeType)) {
      throw new IllegalStateException(mapAttr + " is not a map type but " + type);
    }
    byte[] markedMapKey = tuple.getState(ContinuousRecordsDeserializer.MAP_KEY_STATE_KEY);
    if (markedMapKey == null) {
      tuple.setState(ContinuousRecordsDeserializer.MAP_VALUE_STATE_KEY, in, offset, length);
      return length;
    }

    if (ByteUtils.equals(in, offset, length,
        MapKeyFormatter.EMPTY_MARK, 0, MapKeyFormatter.EMPTY_MARK.length)) {
      tuple.putAttribute(mapAttr, Collections.emptyMap());
    } else {
      MapAttributeType mapType = (MapAttributeType) type;
      Object key = mapType.getKeyType().deserialize(markedMapKey);
      Object o = tuple.getAttribute(mapAttr);
      Map m = o == null ? new HashMap() : (Map) o;
      Object v = mapType.getValueType().deserialize(in, offset, length);
      m.put(key, v);
      tuple.putAttribute(mapAttr, m);
    }
    return length;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws SerdeException {
    Object attrVal = tuple.getAttribute(mapAttr);
    if (!(attrVal instanceof Map)) {
      throw new IllegalArgumentException(mapAttr + " is not a map but " + attrVal);
    }
    Map<?, ?> m = (Map) attrVal;
    if (m.isEmpty()) {
      serializer.write(null, MapKeyFormatter.EMPTY_MARK);
      return;
    }

    final MapAttributeType mapType = (MapAttributeType) tuple.getAttributeType(mapAttr);
    final AttributeType keyType = mapType.getKeyType();
    final AttributeType valueType = mapType.getValueType();
    final TreeBasedTupleSerializer tts = castSerializer(serializer);
    final TreeNode<byte[]> parent = tts.getCurrentParent();

    final byte[] serializedMapKey = findMapKey(mapAttr, parent);
    if (serializedMapKey == null) {
      for (Map.Entry e : m.entrySet()) {
        byte[] byteKey = keyType.serialize(e.getKey());
        Object v = encode(e.getValue());
        serializer.write(TreeNode.mapKeyDesc(mapAttr, byteKey), valueType.serialize(v));
      }
    } else {
      Object mapKey = keyType.deserialize(serializedMapKey);
      Object v = encode(m.get(mapKey));
      byte[] byteVal = valueType.serialize(v);
      serializer.write(null, byteVal);
    }
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction) {
    serializer.write(null, TrueSegment.INSTANCE);
  }


  @Override
  public boolean containsAttribute(String attr) {
    if (attr == null) {
      return false;
    }
    return attr.equals(mapAttr);
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  public String toString() {
    return String.format("%s[]", FORMATTER_TYPE);
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(MAP_ATTR_PROPKEY, mapAttr);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    this.mapAttr = (String) props.get(MAP_ATTR_PROPKEY);
  }

  private Object encode(Object v) {
    if (v == null) {
      return null;
    }
    return v;
  }

  protected TreeBasedTupleSerializer castSerializer(TupleSerializer serializer) {
    if (serializer instanceof TupleSerializerWrapper) {
      return (TreeBasedTupleSerializer) ((TupleSerializerWrapper) serializer).getWrapped();
    } else {
      return (TreeBasedTupleSerializer) serializer;
    }
  }

  // TODO refactor (unify with MapKeyFormatter)
  protected byte[] findMapKey(String mapAttr, TreeNode<byte[]> parent) {
    String desc = parent.getType();
    if (desc != null) {
      byte[] key = TreeNode.parseMapKeyType(mapAttr, desc);
      if (key != null) {
        return key;
      }
    }
    if (parent.getParent() == null) {
      return null;
    }
    return findMapKey(mapAttr, parent.getParent());
  }

  public static class Factory implements FormatterFactory {

    @Override
    public Formatter create(Map config) {
      Object mapAttr = config.get(MAP_ATTR_PROPKEY);
      MapValueFormatter e = new MapValueFormatter();
      e.mapAttr = (String) mapAttr;
      return e;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return MapKeyFormatter.class;
    }
  }
}
