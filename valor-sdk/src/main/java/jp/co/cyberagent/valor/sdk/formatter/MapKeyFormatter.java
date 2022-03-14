package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.serde.ContinuousRecordsDeserializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeNode;
import jp.co.cyberagent.valor.spi.exception.ValorException;
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

public class MapKeyFormatter extends DisassembleFormatter {

  public static final String FORMATTER_TYPE = "mapKey";

  public static final String MAP_ATTR_PROPKEY = "mapAttr";

  public static final byte[] EMPTY_MARK = ByteUtils.toBytes("\u0000");
  private String mapAttr;

  @Deprecated
  public static MapKeyFormatter create(String mapAttr) {
    MapKeyFormatter e = new MapKeyFormatter();
    e.mapAttr = mapAttr;
    return e;
  }

  private MapKeyFormatter() {
  }

  public MapKeyFormatter(String mapAttr) {
    this.mapAttr = mapAttr;
  }

  @Override
  public Order getOrder() {
    return Order.NORMAL;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, final TupleDeserializer target) {
    // TODO check in configuration
    AttributeType type = target.getRelation().getAttributeType(mapAttr);
    if (!(type instanceof MapAttributeType)) {
      throw new IllegalStateException(mapAttr + " is not a map type but " + type);
    }

    byte[] mapValue = target.getState(ContinuousRecordsDeserializer.MAP_VALUE_STATE_KEY);
    if (mapValue == null) {
      target.setState(ContinuousRecordsDeserializer.MAP_KEY_STATE_KEY, in, offset, length);
      return length;
    }

    if (ByteUtils.equals(in, offset, length, EMPTY_MARK, 0, EMPTY_MARK.length)) {
      target.putAttribute(mapAttr, Collections.emptyMap());
    } else {
      MapAttributeType mapType = (MapAttributeType) type;
      Object k = mapType.getKeyType().deserialize(in, offset, length);
      Object v = mapType.getValueType().deserialize(mapValue);
      Object o = target.getAttribute(mapAttr);
      Map m = o == null ? new HashMap() : (Map) o;
      m.put(k,v);
      target.putAttribute(mapAttr, m);
    }
    return length;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    Object o = tuple.getAttribute(mapAttr);
    if (!(o instanceof Map)) {
      throw new IllegalArgumentException(String.format("%s = %s is not a map", mapAttr, o));
    }

    Map<?, ?> m = (Map<?, ?>) o;
    if (m.isEmpty()) {
      serializer.write(TreeNode.mapKeyDesc(mapAttr, EMPTY_MARK), EMPTY_MARK);
      return;
    }

    final AttributeType keyType = ((MapAttributeType) tuple.getAttributeType(mapAttr)).getKeyType();
    final TreeBasedTupleSerializer tts = castSerializer(serializer);
    final TreeNode<byte[]> parent = tts.getCurrentParent();

    final byte[] serializedMapKey = findMapKey(mapAttr, parent);
    if (serializedMapKey == null) {
      for (Map.Entry e : m.entrySet()) {
        byte[] byteKey = keyType.serialize(e.getKey());
        serializer.write(TreeNode.mapKeyDesc(mapAttr, byteKey), keyType.serialize(e.getKey()));
      }
    } else {
      Object key = keyType.deserialize(serializedMapKey);
      serializer.write(null, serializedMapKey);
    }
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction) {
    // FIXME create filtering fragment
    serializer.write(TreeNode.mapKeyDesc(mapAttr, EMPTY_MARK), TrueSegment.INSTANCE);
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
    HashMap<String, Object> props = new HashMap<>();
    props.put(MAP_ATTR_PROPKEY, mapAttr);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    this.mapAttr = (String) props.get(MAP_ATTR_PROPKEY);
  }

  private TreeBasedTupleSerializer castSerializer(TupleSerializer serializer) {
    if (serializer instanceof TupleSerializerWrapper) {
      return (TreeBasedTupleSerializer) ((TupleSerializerWrapper) serializer).getWrapped();
    } else {
      return (TreeBasedTupleSerializer) serializer;
    }
  }

  // TODO refactor (unify with MapVAlueFormatter)
  private byte[] findMapKey(String mapAttr, TreeNode<byte[]> parent) {
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
      if (mapAttr == null) {
        throw new IllegalArgumentException(MAP_ATTR_PROPKEY + " is not set");
      }
      return create((String) mapAttr);
    }

    public MapKeyFormatter create(String mapAttr) {
      MapKeyFormatter e = new MapKeyFormatter();
      e.mapAttr = mapAttr;
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
