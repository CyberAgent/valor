package jp.co.cyberagent.valor.sdk.holder;

import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.HolderFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.BetweenSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.QuerySerializerWrapper;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class FixedLengthHolder extends Holder {
  public static final String NAME = "fixLength";

  public static final String LENGTH_PROPKEY = "length";
  private int length;

  public static FixedLengthHolder create(int length, Formatter formatter) {
    FixedLengthHolder f = new FixedLengthHolder();
    f.length = length;
    f.formatter = formatter;
    return f;
  }

  @Override
  public TupleSerializer bytesWrapper(TupleSerializer wrapped) throws SerdeException {
    return wrapped;
  }

  @Override
  public QuerySerializer filterWrapper(QuerySerializer wrapped) throws SerdeException {
    return new QuerySerializerWrapper(wrapped) {
      @Override
      public void write(String type, FilterSegment slice) {
        if (slice instanceof GreaterThanSegment) {
          GreaterThanSegment gts = (GreaterThanSegment) slice;
          if (!gts.isIncludeBorder()) {
            slice = gts.copyWithNewValue(ByteUtils.increment(gts.getValue()));
          }
        } else if (slice instanceof BetweenSegment) {
          BetweenSegment bs = (BetweenSegment) slice;
          if (!bs.isIncludeMin()) {
            byte[] newMin = ByteUtils.increment(bs.getMin());
            slice = new BetweenSegment(bs.getOrigin(),
                true, newMin, bs.isIncludeMax(), bs.getMax());
          }
        }
        wrapped.write(type, slice);
      }
    };
  }

  @Override
  public Order getOrder() {
    return formatter.getOrder();
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int l, TupleDeserializer target)
      throws SerdeException {
    formatter.cutAndSet(in, offset, length, target);
    return length;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return String.format("%s('%d'){%s}", NAME, length, formatter.toString());
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(LENGTH_PROPKEY, length);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    this.length = (int) props.get(LENGTH_PROPKEY);
  }

  public static class Factory implements HolderFactory {
    @Override
    public Holder create(Map config) {
      Object l = config.get(LENGTH_PROPKEY);
      if (l == null) {
        throw new IllegalArgumentException(LENGTH_PROPKEY + " is not set");
      }
      return create((int) l);
    }

    public FixedLengthHolder create(int length) {
      FixedLengthHolder f = new FixedLengthHolder();
      f.length = length;
      return f;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Holder> getProvidedClass() {
      return FixedLengthHolder.class;
    }
  }
}
