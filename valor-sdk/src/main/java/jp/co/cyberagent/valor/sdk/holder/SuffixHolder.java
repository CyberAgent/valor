package jp.co.cyberagent.valor.sdk.holder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.HolderFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.BetweenSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.RegexpMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.SingleValueFilterSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.QuerySerializerWrapper;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializerWrapper;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SuffixHolder extends Holder {

  public static final String NAME = "suffix";
  public static final String SUFFIX_PROPKEY = "suffix";
  public static final String FROM_HEAD_PROPKEY = "fromHead";
  public static final boolean DEFAULT_FROM_HEAD = true;
  static final Logger LOG = LoggerFactory.getLogger(SuffixHolder.class);

  private byte[] suffix;

  private boolean fromHead = DEFAULT_FROM_HEAD;

  public static SuffixHolder create(String suffix, Formatter formatter) {
    return create(suffix, DEFAULT_FROM_HEAD, formatter);
  }

  public static SuffixHolder create(String suffix, boolean fromHead, Formatter formatter) {
    SuffixHolder fmt = new SuffixHolder();
    fmt.suffix = ByteUtils.toBytes(suffix);
    fmt.fromHead = fromHead;
    fmt.formatter = formatter;
    return fmt;
  }

  @Override
  public TupleSerializer bytesWrapper(TupleSerializer wrapped) throws SerdeException {
    return new TupleSerializerWrapper(wrapped) {
      @Override
      public void write(String type, byte[]... values) {
        wrapped.write(type, values[0], suffix);
      }
    };
  }

  @Override
  public QuerySerializer filterWrapper(QuerySerializer wrapped) throws SerdeException {
    return new QuerySerializerWrapper(wrapped) {

      // rewrite exclusive predicates to inclusive one
      // since exclusive predicates cannot be pushed down correctly
      // (suffix and original encoded value cannot be distinguished).
      @Override
      public void write(String type, FilterSegment slice) {
        if (slice instanceof CompleteMatchSegment || slice instanceof RegexpMatchSegment) {
          SingleValueFilterSegment svfs = (SingleValueFilterSegment) slice;
          byte[] val = svfs.getValue();
          byte[] newVal = ByteUtils.add(val, suffix);
          slice = svfs.copyWithNewValue(newVal);
        } else if (slice instanceof GreaterThanSegment) {
          GreaterThanSegment gts = (GreaterThanSegment) slice;
          if (!gts.isIncludeBorder()) {
            PredicativeExpression predicate = toInclusivePredicate(gts.getOrigin(), getOrder());
            slice = new GreaterThanSegment(predicate, gts.getValue(), true);
          }
        } else if (slice instanceof BetweenSegment) {
          BetweenSegment bs = (BetweenSegment) slice;
          if (!bs.isIncludeMin()) {
            PredicativeExpression predicate = toInclusivePredicate(bs.getOrigin(), getOrder());
            slice = new BetweenSegment(predicate,
                true, bs.getMin(), bs.isIncludeMax(), bs.getMax());
          }
        }
        wrapped.write(type, slice);
      }
    };
  }

  private PredicativeExpression toInclusivePredicate(PredicativeExpression predicate, Order order) {
    if (Order.RANDOM.equals(order)) {
      return null;
    }
    if (predicate instanceof GreaterthanOperator) {
      GreaterthanOperator gtop = (GreaterthanOperator) predicate;
      return Order.NORMAL.equals(order)
          ? new GreaterthanorequalOperator(gtop.getLeft(), gtop.getRight()) : gtop;
    } else if (predicate instanceof LessthanOperator) {
      LessthanOperator ltop = (LessthanOperator) predicate;
      return Order.NORMAL.equals(order)
          ? ltop : new LessthanorequalOperator(ltop.getLeft(), ltop.getRight());
    } else if (predicate instanceof AndOperator) {
      AndOperator aop = (AndOperator) predicate;
      if (aop.getOperands().size() == 2) {
        PredicativeExpression pred1 = toInclusivePredicate(aop.getOperands().get(0), order);
        PredicativeExpression pred2 = toInclusivePredicate(aop.getOperands().get(1), order);
        if (pred1 != null && pred2 != null) {
          return AndOperator.join(pred1, pred2);
        } else if (pred1 == null && pred2 == null) {
          return null;
        } else {
          return  pred1 == null ? pred2 : pred1;
        }
      }
    }
    LOG.warn("unexpected predicated is embedded in greater than segment: {}", predicate);
    return null;
  }

  @Override
  public Order getOrder() {
    return formatter.getOrder();
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    int index = fromHead
        ? findSuffixFromHead(in, offset, length, suffix)
        : findSuffixFromTail(in, offset, length, suffix);
    if (index < 0) {
      String v = ByteUtils.toStringBinary(in, offset, length);
      throw new SerdeException(v + " does not have suffix: " + suffix + " on " + formatter);
    }
    int l = index - offset;
    l = formatter.cutAndSet(in, offset, l, target);
    return l + suffix.length;
  }

  // implement efficient pattern matching algorithm (e.g. KMP) if long suffix is needed
  private int findSuffixFromHead(byte[] target, int offset, int length, byte[] suffix) {
    for (int i = offset; i < offset + length; i++) {
      if (match(target, i, suffix)) {
        return i;
      }
    }
    return -1;
  }

  private int findSuffixFromTail(byte[] target, int offset, int length, byte[] suffix) {
    for (int i = offset + length - 1; i >= offset; i--) {
      if (match(target, i, suffix)) {
        return i;
      }
    }
    return -1;
  }

  private boolean match(byte[] target, int targetIndex, byte[] suffix) {
    for (int i = 0; i < suffix.length; i++) {
      if (target[targetIndex + i] != suffix[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String toString() {
    return String.format("%s('%s','%s'){%s}",
        NAME, ByteUtils.toString(suffix), fromHead, formatter.toString());
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(SUFFIX_PROPKEY, ByteUtils.toString(suffix));
    props.put(FROM_HEAD_PROPKEY, fromHead);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    this.suffix = ByteUtils.toBytes((String) props.get(SUFFIX_PROPKEY));
    if (props.containsKey(FROM_HEAD_PROPKEY)) {
      Object v = props.get(FROM_HEAD_PROPKEY);
      if (v instanceof String) {
        this.fromHead = Boolean.valueOf((String) v);
      } else if (v instanceof Boolean) {
        this.fromHead = Boolean.valueOf((Boolean) v);
      } else {
        throw new IllegalArgumentException(
            "unsupported type for " + FROM_HEAD_PROPKEY + ": " + v.getClass());
      }
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SuffixHolder that = (SuffixHolder) o;
    return fromHead == that.fromHead && Arrays.equals(suffix, that.suffix);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(fromHead);
    result = 31 * result + Arrays.hashCode(suffix);
    return result;
  }

  public static class Factory implements HolderFactory {

    @Override
    public Holder create(Map config) {
      Object suffix = config.get(SUFFIX_PROPKEY);
      if (suffix == null) {
        throw new IllegalArgumentException(SUFFIX_PROPKEY + " is not set");
      }
      Object fromHead = config.get(FROM_HEAD_PROPKEY);
      if (fromHead == null) {
        fromHead = DEFAULT_FROM_HEAD;
      }
      return create((String) suffix, (Boolean) fromHead);
    }

    public SuffixHolder create(String suffix, boolean fromHead) {
      SuffixHolder fmt = new SuffixHolder();
      fmt.suffix = ByteUtils.toBytes(suffix);
      fmt.fromHead = fromHead;
      return fmt;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Holder> getProvidedClass() {
      return SuffixHolder.class;
    }
  }
}
