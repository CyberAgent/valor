package jp.co.cyberagent.valor.sdk.holder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.HolderFactory;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.FalseSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.IsnullSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.NotMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.RegexpMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.SingleValueFilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.QuerySerializerWrapper;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializerWrapper;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

/**
 * set vInt size prefix
 */
public class VintSizePrefixHolder extends Holder {

  public static final String NAME = "size";

  public static Segment create(Formatter formatter) {
    VintSizePrefixHolder instance = new VintSizePrefixHolder();
    instance.formatter = formatter;
    return instance;
  }

  @Override
  public TupleSerializer bytesWrapper(TupleSerializer wrapped) throws SerdeException {
    return new TupleSerializerWrapper(wrapped) {
      @Override
      public void write(String type, byte[]... values) {
        wrapped.write(type, appendSize(values[0]));
      }
    };
  }

  @Override
  public QuerySerializer filterWrapper(QuerySerializer wrapped) throws SerdeException {
    return new QuerySerializerWrapper(wrapped) {
      @Override
      public void write(String type, FilterSegment slice) {
        if (slice instanceof TrueSegment || slice instanceof FalseSegment) {
          wrapped.write(type, slice);
        } else if (slice instanceof IsnullSegment) {
          wrapped.write(type, new CompleteMatchSegment(null, new byte[] {(byte) -1}));
        } else {
          if (slice instanceof CompleteMatchSegment || slice instanceof NotMatchSegment) {
            SingleValueFilterSegment svfs = (SingleValueFilterSegment) slice;
            byte[] val = svfs.getValue();
            wrapped.write(type, svfs.copyWithNewValue(appendSize(val)));
          } else if (slice instanceof RegexpMatchSegment) {
            RegexpMatchSegment rs = (RegexpMatchSegment) slice;
            byte[] regexp = rs.getValue();
            if (ByteUtils.startsWith(regexp, RegexpMatchSegment.WILDCARD)) {
              wrapped.write(type, slice);
            } else {
              wrapped.write(
                  type, rs.copyWithNewValue(ByteUtils.add(RegexpMatchSegment.WILDCARD, regexp)));
            }
          } else {
            wrapped.write(type, TrueSegment.INSTANCE);
          }
        }
      }
    };
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(in, offset, length);
         DataInputStream dis = new DataInputStream(bais)) {
      int len = ByteUtils.readVInt(dis);
      int vintSize = ByteUtils.getVIntSize(len);
      if (len >= 0) {
        formatter.cutAndSet(in, offset + vintSize, len, target);
        return vintSize + len;
      } else {
        return vintSize;
      }

    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  @Override
  public String getName() {
    return NAME;
  }

  private byte[] appendSize(byte[] value) throws SerdeException {
    if (value == null) {
      return new byte[] {(byte) -1};
    }
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try {
      ByteUtils.writeByteArray(new DataOutputStream(baos), value);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
    return baos.toByteArray();
  }

  @Override
  public String toString() {
    return String.format("%s(){%s}", NAME, formatter.toString());
  }

  @Override
  public Map<String, Object> getProperties() {
    return Collections.emptyMap();
  }

  @Override
  public void setProperties(Map<String, Object> props) {
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof VintSizePrefixHolder)) {
      return false;
    }
    return Objects.equals(formatter, ((VintSizePrefixHolder) obj).formatter);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(formatter);
  }

  public static class Factory implements HolderFactory {

    @Override
    public Holder create(Map config) {
      return new VintSizePrefixHolder();
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Holder> getProvidedClass() {
      return VintSizePrefixHolder.class;
    }
  }
}
