package jp.co.cyberagent.valor.sdk.holder;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jp.co.cyberagent.valor.spi.exception.MismatchByteArrayException;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.HolderFactory;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializerWrapper;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class RegexpHolder extends Holder {

  public static final String PATTERN_PROPKEY = "pattern";

  public static final String NAME = "regexp";
  private Pattern pattern = null;

  public static RegexpHolder create(String regexp, Formatter formatter) {
    RegexpHolder f = new RegexpHolder();
    f.pattern = Pattern.compile(regexp);
    f.formatter = formatter;
    return f;
  }

  private Pattern pattern() {
    return pattern;
  }

  @Override
  public TupleSerializer bytesWrapper(TupleSerializer wrapped) throws SerdeException {
    return new TupleSerializerWrapper(wrapped) {
      @Override
      public void write(String type, byte[]... values) {
        String strVal = ByteUtils.toString(values[0]);
        Matcher matcher = pattern().matcher(strVal);
        if (!matcher.matches()) {
          throw new MismatchByteArrayException(strVal);
        }
        wrapped.write(type, values);
      }
    };
  }

  @Override
  public QuerySerializer filterWrapper(QuerySerializer wrapped) throws SerdeException {
    return wrapped;
  }

  @Override
  public Order getOrder() {
    return formatter.getOrder();
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    String strVal;
    try {
      strVal = new String(in, offset, length, StringAttributeType.CHARSET_NAME);
    } catch (UnsupportedEncodingException e) {
      throw new SerdeException(e);
    }
    Matcher matcher = pattern().matcher(strVal);
    if (matcher.find() && matcher.start() == 0) {
      String match = matcher.group();
      byte[] matchedBytes = ByteUtils.toBytes(match);
      formatter.cutAndSet(in, offset, matchedBytes.length, target);
      return matchedBytes.length;
    }
    throw new MismatchByteArrayException(strVal);
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return String.format("%s('%s'){%s}", NAME, pattern().toString(), formatter.toString());
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(PATTERN_PROPKEY, pattern.pattern());
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    this.pattern = Pattern.compile((String) props.get(PATTERN_PROPKEY));
  }

  public static class Factory implements HolderFactory {

    @Override
    public Holder create(Map config) {
      Object p = config.get(PATTERN_PROPKEY);
      if (p == null) {
        throw new IllegalArgumentException(PATTERN_PROPKEY + " is not set");
      }
      return create((String) p);
    }

    public RegexpHolder create(String regexp) {
      RegexpHolder f = new RegexpHolder();
      f.pattern = Pattern.compile(regexp);
      return f;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Holder> getProvidedClass() {
      return RegexpHolder.class;
    }
  }
}
