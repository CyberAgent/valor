package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Collection;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import jp.co.cyberagent.valor.spi.util.Pair;

public class ReverseLongFormatter extends AbstractAttributeValueFormatter {

  public static final String FORMATTER_TYPE = "reverseLong";

  @Override
  public Order getOrder() {
    return Order.REVERSE;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    Long v = ByteUtils.toLong(in, offset);
    target.putAttribute(attrName, Long.MAX_VALUE - v);
    return LongAttributeType.SIZE;
  }

  @Override
  protected byte[] serialize(Object attrVal, AttributeType<?> type) {
    Long val = Long.MAX_VALUE - (Long) attrVal;
    return ByteUtils.toBytes(val);
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws SerdeException {
    FilterSegment segment = null;
    for (PrimitivePredicate predicate : conjunction) {
      Long v = extractValue(predicate);
      if (v == null) {
        continue;
      }
      byte[] b = ByteUtils.toBytes(Long.MAX_VALUE - v);
      FilterSegment tmpSegment = null;
      if (predicate instanceof EqualOperator) {
        tmpSegment = new CompleteMatchSegment(predicate, b);
      } else if (predicate instanceof GreaterthanOperator) {
        tmpSegment = new LessThanSegment(predicate, b, false);
      } else if (predicate instanceof GreaterthanorequalOperator) {
        tmpSegment = new LessThanSegment(predicate, b, true);
      } else if (predicate instanceof LessthanOperator) {
        tmpSegment = new GreaterThanSegment(predicate, b, false);
      } else if (predicate instanceof LessthanorequalOperator) {
        tmpSegment = new GreaterThanSegment(predicate, b, true);
      }
      if (tmpSegment != null) {
        segment = segment == null ? tmpSegment : segment.mergeByAnd(tmpSegment);
      }
    }
    if (segment == null) {
      segment = TrueSegment.INSTANCE;
    }
    serializer.write(null, segment);
  }
  
  private Long extractValue(PrimitivePredicate predicate) {
    if (predicate instanceof BinaryPrimitivePredicate) {
      Pair<String, Object> attrAndVal =
          ((BinaryPrimitivePredicate) predicate).getAttributeAndConstantIfExists();
      if (!attrName.equals(attrAndVal.getFirst())) {
        return null;
      }
      Object v = attrAndVal.getSecond();
      if (v instanceof Long) {
        return (Long) v;
      }
    }
    return null;
  }

  @Override
  protected FilterSegment convert(FilterSegment fragment) {
    throw new UnsupportedOperationException(
        "should not be invoked because of outer method is overridden");
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  public String toString() {
    return String.format("%s(){%s}", FORMATTER_TYPE, attrName);
  }

  public static class Factory implements FormatterFactory {
    @Override
    public Formatter create(Map config) {
      Object attrName = config.get(ATTRIBUTE_NAME_PROPKEY);
      if (attrName == null) {
        throw new IllegalArgumentException(ATTRIBUTE_NAME_PROPKEY + " is not set");
      }
      return create((String) attrName);
    }

    public ReverseLongFormatter create(String attrName) {
      ReverseLongFormatter elm = new ReverseLongFormatter();
      elm.attrName = attrName;
      return elm;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return ReverseLongFormatter.class;
    }
  }
}
