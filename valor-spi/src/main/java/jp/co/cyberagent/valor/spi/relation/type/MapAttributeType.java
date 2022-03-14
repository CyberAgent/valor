package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

@SuppressWarnings({"rawtypes", "unchecked"})
public class MapAttributeType extends AttributeType<Map> {

  public static final String NAME = "map";
  private AttributeType keyType = null;
  private AttributeType valueType = null;

  public static MapAttributeType create(AttributeType keyType, AttributeType valueType) {
    MapAttributeType mapType = new MapAttributeType();
    mapType.keyType = keyType;
    mapType.valueType = valueType;
    return mapType;
  }

  @Override
  protected int doWrite(DataOutput out, Object o) throws IOException {
    if (!(o instanceof Map)) {
      throw new IllegalTypeException("Map is expected but " + o.getClass().getCanonicalName());
    }

    Map m = (Map) o;
    SortedMap<byte[], Object> sortBuf = toSortedMap(m);
    int size = 0;
    for (Map.Entry<byte[], Object> e : sortBuf.entrySet()) {
      byte[] k = e.getKey();
      ByteUtils.writeVInt(out, k.length);
      out.write(k);
      size = size + ByteUtils.getVIntSize(k.length) + k.length;
      if (e.getValue() == null) {
        ByteUtils.writeVInt(out, NULL_BYTES_SIZE);
        size += NULL_BYTES_VINT_SIZE;
      } else {
        byte[] v = valueType.serialize(e.getValue());
        ByteUtils.writeVInt(out, v.length);
        out.write(v);
        size = size + ByteUtils.getVIntSize(v.length) + v.length;
      }
    }
    return size;
  }

  public SortedMap<byte[], Object> toSortedMap(Map m) throws SerdeException {
    SortedMap<byte[], Object> sortBuf = new TreeMap(ByteUtils.BYTES_COMPARATOR);
    for (Object k : m.keySet()) {
      sortBuf.put(keyType.serialize(k), m.get(k));
    }
    return sortBuf;
  }

  @Override
  protected Map doRead(byte[] in, int offset, int length) throws IOException {
    Map m = new HashMap();
    int position = offset;
    while (position < offset + length) {
      int keySize = ByteUtils.readVInt(in, position);
      position += ByteUtils.getVIntSize(keySize);
      Object key = keyType.read(in, position, keySize);
      position += keySize;
      int valSize = ByteUtils.readVInt(in, position);
      position += ByteUtils.getVIntSize(valSize);
      if (valSize < 0) {
        m.put(key, null);
      } else {
        m.put(key, valueType.read(in, position, valSize));
        position += valSize;
      }
    }
    return m;
  }

  @Override
  public int getSize() {
    return -1;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Class getRepresentedClass() {
    return Map.class;
  }

  @Override
  public List<AttributeType> getGenericParameterValues() {
    return Arrays.asList(keyType, valueType);
  }

  public AttributeType getKeyType() {
    return keyType;
  }

  public AttributeType getValueType() {
    return valueType;
  }

  @Override
  public void addGenericElementType(AttributeType attributeType) {
    if (keyType == null) {
      this.keyType = attributeType;
      return;
    } else if (valueType == null) {
      this.valueType = attributeType;
      return;
    }
    throw new IllegalStateException(this.getClass()
        .getCanonicalName() + " can have only 2 generic type parameter, previously set to "
        + this.keyType + " and " + this.valueType);
  }

  @Override
  public String toExpression() {
    return String.format("%s<%s,%s>", NAME, keyType.toExpression(), valueType.toExpression());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MapAttributeType that = (MapAttributeType) o;
    return Objects.equals(keyType, that.keyType) && Objects.equals(valueType, that.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyType, valueType);
  }
}
