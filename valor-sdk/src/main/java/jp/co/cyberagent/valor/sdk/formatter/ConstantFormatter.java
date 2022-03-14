package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.exception.MismatchByteArrayException;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class ConstantFormatter extends Formatter {
  public static final String FORMATTER_TYPE = "const";

  public static final String VALUE_PROPKEY = "value";

  public static final String ENCODE_PROPKEY = "encode";

  public static final String ENCODE_BASE64 = "base64";

  static final Base64.Encoder ENCODER = Base64.getEncoder();

  static final Base64.Decoder DECODER = Base64.getDecoder();

  private byte[] value;

  public ConstantFormatter() {
  }

  public ConstantFormatter(Map<String, Object> config) {
    setProperties(config);
  }

  public ConstantFormatter(String value) {
    this(ByteUtils.toBytes(value));
  }

  public ConstantFormatter(byte[] value) {
    this.value = value;
  }

  public static ConstantFormatter create(String value) {
    return new ConstantFormatter(ByteUtils.toBytes(value));
  }

  @Override
  public Order getOrder() {
    return Order.NORMAL;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    if (!ByteUtils.equals(in, offset, value.length, value, 0, value.length)) {
      throw new MismatchByteArrayException(value, ByteUtils.copy(in, offset, value.length));
    }
    return value.length;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) {
    serializer.write(null, value);
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction) {
    serializer.write(null, new CompleteMatchSegment(null, value));
  }

  @Override
  public String toString() {
    // TODO handle more type of value other than String
    return String.format("%s['%s']", FORMATTER_TYPE, ByteUtils.toString(value));
  }

  @Override
  public boolean containsAttribute(String attr) {
    return false;
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  public byte[] getValue() {
    return value;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(VALUE_PROPKEY, ENCODER.encodeToString(value));
    props.put(ENCODE_PROPKEY, "base64");
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    Object encode = props.get(ENCODE_PROPKEY);
    String value = (String) props.get(VALUE_PROPKEY);
    if (encode == null) {
      this.value = ByteUtils.toBytes(value);
    } else if (ENCODE_BASE64.equals(encode)) {
      this.value = DECODER.decode((String) props.get(VALUE_PROPKEY));
    } else {
      throw new IllegalArgumentException("unsupported encoding " + encode);
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ConstantFormatter)) {
      return false;
    }
    boolean v = Arrays.equals(value, ((ConstantFormatter) obj).value);
    return Arrays.equals(value, ((ConstantFormatter) obj).value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }

  public static class Factory implements FormatterFactory {
    @Override
    public Formatter create(Map config) {
      Object v = config.get(VALUE_PROPKEY);
      if (v == null) {
        throw new IllegalArgumentException(VALUE_PROPKEY + " is not set");
      }
      return new ConstantFormatter(config);
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return ConstantFormatter.class;
    }
  }
}
