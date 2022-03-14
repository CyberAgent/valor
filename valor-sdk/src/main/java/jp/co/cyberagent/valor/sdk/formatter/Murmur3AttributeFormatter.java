package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import jp.co.cyberagent.valor.spi.util.MurmurHash3;

/**
 * set murmur3_32 hash (int) of attribute value
 */
public class Murmur3AttributeFormatter extends AbstractHashFormatter {

  public static final String FORMATTER_TYPE = "murmur3";

  @Deprecated
  public static Murmur3AttributeFormatter create(String... attrNames) {
    Murmur3AttributeFormatter formatter = new Murmur3AttributeFormatter();
    formatter.setProperties(new HashMap<String, Object>() {
      {
        put(ATTRIBUTES_NAME_PROPKEY, Arrays.asList(attrNames));
      }
    });
    return formatter;
  }

  @Override
  protected int getMaxLength() {
    return ByteUtils.SIZEOF_INT;
  }

  @Override
  protected byte[] calculateHash(byte[] value) {
    return ByteUtils.toBytes(MurmurHash3.hash(value));
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  public static class Factory implements FormatterFactory {

    public Formatter create(int length, String... attrName) {
      return create(length, Arrays.asList(attrName));
    }

    public Formatter create(int length, List<String> attrNames) {
      return create(new HashMap<String, Object>() {
        {
          put(LENGTH_PROPKEY, length);
          put(ATTRIBUTES_NAME_PROPKEY, attrNames);
        }
      });
    }

    @Override
    public Formatter create(Map config) {
      Murmur3AttributeFormatter formatter = new Murmur3AttributeFormatter();
      formatter.setProperties(config);
      return formatter;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return Murmur3AttributeFormatter.class;
    }
  }
}
