package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.apache.commons.codec.digest.DigestUtils;

public class Sha1AttributeFormatter extends AbstractHashFormatter {

  public static final String FORMATTER_TYPE = "sha1";
  public static final int SHA1_LENGTH = 40;

  @Deprecated
  public static Sha1AttributeFormatter create(String attrName, int length) {
    Sha1AttributeFormatter elm = new Sha1AttributeFormatter();
    elm.setProperties(new HashMap<String, Object>() {
      {
        put(LENGTH_PROPKEY, length);
        put(ATTRIBUTES_NAME_PROPKEY, Collections.singletonList(attrName));
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
    return SHA1_LENGTH;
  }

  @Override
  protected byte[] calculateHash(byte[] value) {
    return ByteUtils.toBytes(DigestUtils.sha1Hex(value));
  }

  @Override
  public String toString() {
    return String.format("%s('%s','%s')", FORMATTER_TYPE, getName(), length);
  }

  public static class Factory implements FormatterFactory {

    public Formatter create(String attrName, int length) {
      return create(new HashMap<String, Object>() {
        {
          put(LENGTH_PROPKEY, length);
          put(ATTRIBUTES_NAME_PROPKEY, Arrays.asList(attrName));
        }
      });
    }

    @Override
    public Formatter create(Map config) {
      Sha1AttributeFormatter formatter = new Sha1AttributeFormatter();
      formatter.setProperties(config);
      return formatter;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return Sha1AttributeFormatter.class;
    }
  }
}
