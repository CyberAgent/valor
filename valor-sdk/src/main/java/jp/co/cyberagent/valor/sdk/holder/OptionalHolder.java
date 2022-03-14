package jp.co.cyberagent.valor.sdk.holder;

import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.HolderFactory;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.QuerySerializerWrapper;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializerWrapper;

/**
 * OptionalHolder is a Holder that is used in the last parts of format to express optional Segment.
 * If there is no data, no bytes are written. If there is data, the data should be in the format of
 * specified child Segment and this class does not guarantee the existence and the length of the
 * Segment.
 */
public class OptionalHolder extends Holder {
  public static final String NAME = "optional";
  private Segment segment;

  public static OptionalHolder create(Segment segment) {
    OptionalHolder fmt = new OptionalHolder();
    fmt.segment = segment;
    fmt.formatter = segment.getFormatter();
    return fmt;
  }

  public static OptionalHolder create() {
    return new OptionalHolder();
  }

  @Override
  public TupleSerializer bytesWrapper(TupleSerializer wrapped) throws SerdeException {
    return new TupleSerializerWrapper(wrapped) {
      @Override
      public void write(String type, byte[]... values) {
        if (values[0] == null) {
          return;
        }
        if (segment instanceof Holder) {
          ((Holder) segment).bytesWrapper(wrapped).write(type, values);
          return;
        }
        wrapped.write(type, values[0]);
      }
    };
  }

  @Override
  public QuerySerializer filterWrapper(QuerySerializer wrapped) throws SerdeException {
    return new QuerySerializerWrapper(wrapped) {
      @Override
      public void write(String type, FilterSegment slice) {
        wrapped.write(type, slice);
      }
    };
  }

  @Override
  public Order getOrder() {
    return segment.getOrder();
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    if (in.length == 0) {
      return 0;
    }
    return segment.cutAndSet(in, offset, length, target);
  }

  @Override
  public Segment setFormatter(Formatter formatter) {
    this.formatter = formatter;
    this.segment = formatter;
    return this;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return String.format("%s{%s}", NAME, segment.toString());
  }

  @Override
  public Map<String, Object> getProperties() {
    return new HashMap<>();
  }

  @Override
  public void setProperties(Map<String, Object> props) {}

  public static class Factory implements HolderFactory {

    @Override
    public Holder create(Map config) {
      return create();
    }

    public OptionalHolder create() {
      return OptionalHolder.create();
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Holder> getProvidedClass() {
      return OptionalHolder.class;
    }
  }
}
