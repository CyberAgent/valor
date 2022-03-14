package jp.co.cyberagent.valor.hbase.formatter;

import com.google.common.annotations.VisibleForTesting;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import jp.co.cyberagent.valor.sdk.formatter.MapKeyFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MapValueFormatter;
import jp.co.cyberagent.valor.sdk.serde.ContinuousRecordsDeserializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeNode;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

/**
 * must follow AttributeNameSchemaELemement.
 */
public class MapTtlValueFormatter extends MapValueFormatter {

  public static final String FORMATTER_TYPE = "mapTtlValue";

  public static final String BASE_TTL_PROPKEY = "baseTtl";

  private long baseTtl;

  private MapTtlValueFormatter() {
  }

  public MapTtlValueFormatter(String mapAttr, long baseTtl) {
    this.mapAttr = mapAttr;
    this.baseTtl = baseTtl;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer tuple)
      throws SerdeException {
    // TODO check in configuration
    AttributeType type = tuple.getRelation().getAttributeType(mapAttr);
    if (!(type instanceof MapAttributeType)) {
      throw new IllegalStateException(mapAttr + " is not a map type but " + type);
    }
    MapAttributeType mapType = (MapAttributeType) type;
    if (!(mapType.getValueType() instanceof LongAttributeType)) {
      throw new IllegalTypeException("long is expected but " + mapType.getValueType().getClass());
    }
    byte[] markedMapKey = tuple.getState(ContinuousRecordsDeserializer.MAP_KEY_STATE_KEY);
    if (markedMapKey == null) {
      length = Long.BYTES;
      tuple.setState(ContinuousRecordsDeserializer.MAP_VALUE_STATE_KEY, in, offset, length);
      return length;
    }

    if (ByteUtils.equals(in,
        offset,
        length,
        MapKeyFormatter.EMPTY_MARK,
        0,
        MapKeyFormatter.EMPTY_MARK.length)) {
      tuple.putAttribute(mapAttr, Collections.emptyMap());
    } else {
      Object key = mapType.getKeyType().deserialize(markedMapKey);
      Object o = tuple.getAttribute(mapAttr);
      Map m = o == null ? new HashMap() : (Map) o;
      m.put(key, decodeAndConvert(mapType, in, offset, length));
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
    if (!(valueType instanceof LongAttributeType)) {
      throw new IllegalTypeException("long is expected but " + valueType.getClass());
    }
    final TreeBasedTupleSerializer tts = castSerializer(serializer);
    final TreeNode<byte[]> parent = tts.getCurrentParent();

    final byte[] serializedMapKey = findMapKey(mapAttr, parent);
    if (serializedMapKey == null) {
      for (Map.Entry e : m.entrySet()) {
        byte[] byteKey = keyType.serialize(e.getKey());
        Long v = encode((Long) e.getValue());
        serializer.write(TreeNode.mapKeyDesc(mapAttr, byteKey), valueType.serialize(v));
      }
    } else {
      Object mapKey = keyType.deserialize(serializedMapKey);
      Long v = encode((Long) m.get(mapKey));
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
    return mapAttr.equals(attr);
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
    props.put(BASE_TTL_PROPKEY, baseTtl);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    this.mapAttr = (String) props.get(MAP_ATTR_PROPKEY);
    this.baseTtl = (long) props.get(BASE_TTL_PROPKEY);
  }

  private Long encode(Long v) {
    if (v == null) {
      return null;
    }
    return createTimestamp(v);
  }

  private Long decodeAndConvert(MapAttributeType type, byte[] in, int offset, int length) {
    LongAttributeType valueType = (LongAttributeType) type.getValueType();
    Long v = valueType.deserialize(in, offset, length);
    return createTtl(v);
  }

  @VisibleForTesting
  Long createTtl(Long timestamp) {
    long currentTime = System.currentTimeMillis();
    return TimeUnit.MILLISECONDS.toSeconds(timestamp - currentTime + (baseTtl * 1000L));
  }

  @VisibleForTesting
  Long createTimestamp(Long ttl) {
    if (ttl <= 0L) {
      throw new IllegalArgumentException("ttl must be grater than 0");
    }
    if (ttl > baseTtl) {
      throw new IllegalArgumentException("ttl:" + ttl + " is larger than baseTtl:" + baseTtl);
    }
    long currentTime = System.currentTimeMillis();
    return currentTime + (ttl * 1000L) - (baseTtl * 1000L);
  }

  public static class Factory implements FormatterFactory {

    @Override
    public Formatter create(Map config) {
      Object mapAttr = config.get(MAP_ATTR_PROPKEY);
      if (mapAttr == null) {
        throw new IllegalArgumentException(MAP_ATTR_PROPKEY + " is not set");
      }
      Object baseTtl = config.get(BASE_TTL_PROPKEY);
      if (baseTtl == null) {
        throw new IllegalArgumentException(BASE_TTL_PROPKEY + " is not set");
      }
      return create((String) mapAttr, ((Number) baseTtl).longValue());
    }

    public MapTtlValueFormatter create(String mapAttr, long baseTtl) {
      if (baseTtl <= 0L) {
        throw new IllegalArgumentException("baseTtl must be grater than 0");
      }
      MapTtlValueFormatter e = new MapTtlValueFormatter();
      e.mapAttr = mapAttr;
      e.baseTtl = baseTtl;
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
