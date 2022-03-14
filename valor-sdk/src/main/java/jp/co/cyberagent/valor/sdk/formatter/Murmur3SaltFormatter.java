package jp.co.cyberagent.valor.sdk.formatter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import jp.co.cyberagent.valor.spi.util.MurmurHash3;

/**
 * set murmur3_32 hash (int) of attribute value
 */
public class Murmur3SaltFormatter extends SaltFormatter {

  public static final String FORMATTER_TYPE = "murmur3salt";

  public static final String ATTRIBUTES_NAME_PROPKEY = "attrs";
  public static final String RANGE_PROPKEY = "range";

  private int range;
  private List<String> attrNames;

  public Murmur3SaltFormatter(Map<String, Object> props) {
    setProperties(props);
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    return ByteUtils.SIZEOF_BYTE;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    byte[] fragmentValue;
    if (attrNames.size() == 1) {
      String attr = attrNames.get(0);
      AttributeType type = tuple.getAttributeType(attr);
      fragmentValue = type.serialize(tuple.getAttribute(attr));
    } else {
      List<byte[]> fragmentValues = new ArrayList<>(attrNames.size());
      int totalLength = 0;
      for (String attrName: attrNames) {
        AttributeType type = tuple.getAttributeType(attrName);
        byte[] value = type.serialize(tuple.getAttribute(attrName));
        fragmentValues.add(value);
        totalLength += value.length;
      }
      ByteBuffer buf = ByteBuffer.allocate(totalLength);
      fragmentValues.stream().forEach(buf::put);
      fragmentValue = buf.array();
    }
    byte[] hash = calculateHash(fragmentValue);
    byte salt = (byte)(Byte.toUnsignedInt(hash[0]) % range);
    serializer.write(null, new byte[]{salt});
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws SerdeException {
    for (int i = 0; i < range; i++) {
      serializer.write(null, new CompleteMatchSegment(null, new byte[]{(byte) i}));
    }
  }

  protected byte[] calculateHash(byte[] value) {
    return ByteUtils.toBytes(MurmurHash3.hash(value));
  }

  @Override
  public boolean containsAttribute(String attr) {
    return false;
  }

  @Override
  public int getSaltSize() {
    return 1;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(RANGE_PROPKEY, range);
    props.put(ATTRIBUTES_NAME_PROPKEY, attrNames);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    if (props.containsKey(RANGE_PROPKEY)) {
      range = (int) props.get(RANGE_PROPKEY);
      if (range > Byte.MAX_VALUE) {
        // TODO support upto 256
        throw new IllegalArgumentException("unsupported range size " + range);
      }
    } else {
      throw new IllegalArgumentException(RANGE_PROPKEY + " is not specified");
    }

    if (props.containsKey(ATTRIBUTES_NAME_PROPKEY)) {
      this.attrNames = (List) props.get(ATTRIBUTES_NAME_PROPKEY);
    }
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  public static class Factory implements FormatterFactory {

    @Override
    public Formatter create(Map config) {
      return new Murmur3SaltFormatter(config);
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return Murmur3SaltFormatter.class;
    }
  }
}
