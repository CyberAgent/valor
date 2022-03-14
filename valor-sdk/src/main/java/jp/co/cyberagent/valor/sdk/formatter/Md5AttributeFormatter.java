package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import org.apache.commons.codec.digest.DigestUtils;

public class Md5AttributeFormatter extends AbstractHashFormatter {

  public static final String FORMATTER_TYPE = "md5";
  public static final int MD5_LENGTH = 16;

  @Deprecated
  public static Md5AttributeFormatter create(int length, String... attrNames) {
    Md5AttributeFormatter elm = new Md5AttributeFormatter();
    elm.setProperties(new HashMap<String, Object>() {
      {
        put(LENGTH_PROPKEY, length);
        put(ATTRIBUTES_NAME_PROPKEY, Arrays.asList(attrNames));
      }
    });
    return elm;
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  protected int getMaxLength() {
    return MD5_LENGTH;
  }

  @Override
  protected byte[] calculateHash(byte[] value) {
    return DigestUtils.md5(value);
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
      Md5AttributeFormatter formatter = new Md5AttributeFormatter();
      formatter.setProperties(config);
      return formatter;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return Md5AttributeFormatter.class;
    }
  }
}
