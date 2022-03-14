package jp.co.cyberagent.valor.hbase.formatter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class TtlFormatter extends Formatter {

  public static final String FORMATTER_TYPE = "ttl";
  public static final String ATTRIBUTE_NAME_PROPKEY = "attr";
  public static final String BASE_TTL_PROPKEY = "baseTtl";
  private String attrName;
  private long baseTtl;

  @Deprecated
  public static TtlFormatter create(String attrName, long baseTtl) {
    TtlFormatter elm = new TtlFormatter();
    elm.attrName = attrName;
    elm.baseTtl = baseTtl;
    return elm;
  }

  @Override
  public Order getOrder() {
    return Order.NORMAL;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    long ttl = ByteUtils.toLong(in, offset);
    ttl = createTtl(ttl, baseTtl);
    target.putAttribute(attrName, ttl);
    return ByteUtils.SIZEOF_LONG;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    AttributeType type = tuple.getAttributeType(attrName);
    if (!(type instanceof LongAttributeType)) {
      throw new IllegalTypeException("long is expected but " + type.getClass());
    }
    long ttl = (long) tuple.getAttribute(attrName);
    serializer.write(null,type.serialize(createTimestamp(ttl, baseTtl)));
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws SerdeException {
    List<BinaryPrimitivePredicate> predicates = filterByAttrName(conjunction);
    if (predicates.isEmpty()) {
      serializer.write(null, TrueSegment.INSTANCE);
    } else {
      FilterSegment slice = predicates.get(0).buildFilterFragment();
      slice = convert(slice);
      for (int i = 1; i < predicates.size(); i++) {
        FilterSegment f = predicates.get(i).buildFilterFragment();
        slice = slice.mergeByAnd(convert(f));
      }
      serializer.write(null, slice);
    }
  }

  private List<BinaryPrimitivePredicate> filterByAttrName(
      Collection<PrimitivePredicate> conjunction) {
    return conjunction.stream().filter(p -> p instanceof BinaryPrimitivePredicate)
        .map(BinaryPrimitivePredicate.class::cast)
        .filter(p -> attrName.equals(p.getAttributeIfUnaryPredicate()))
        .collect(Collectors.toList());
  }

  protected FilterSegment convert(FilterSegment fragment) {
    if (fragment instanceof CompleteMatchSegment) {
      CompleteMatchSegment cmf = (CompleteMatchSegment) fragment;
      byte[] value = cmf.getValue();
      return cmf.copyWithNewValue(ByteUtils.toBytes(createTtl(ByteUtils.toLong(value),baseTtl)));
    }
    return TrueSegment.INSTANCE;
  }

  private long createTtl(long timestamp, long baseTtl) {
    long currentTime = System.currentTimeMillis();
    return TimeUnit.MILLISECONDS.toSeconds(timestamp - currentTime + (baseTtl * 1000L));
  }

  private long createTimestamp(long ttl, long baseTtl) {
    if (ttl <= 0L) {
      throw new IllegalArgumentException("ttl must be grater than 0");
    }
    if (ttl > baseTtl) {
      throw new IllegalArgumentException("ttl:" + ttl + " is larger than baseTtl:" + baseTtl);
    }
    long currentTime = System.currentTimeMillis();
    return currentTime + (ttl * 1000L) - (baseTtl * 1000L);
  }

  @Override
  public boolean containsAttribute(String attr) {
    if (attr == null) {
      return false;
    }
    return attr.equals(attrName);
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(ATTRIBUTE_NAME_PROPKEY, attrName);
    props.put(BASE_TTL_PROPKEY, baseTtl);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    this.attrName = (String) props.get(ATTRIBUTE_NAME_PROPKEY);
    this.baseTtl = (long) props.get(BASE_TTL_PROPKEY);
  }

  public static class Factory implements FormatterFactory {

    @Override
    public Formatter create(Map config) {
      Object attrName = config.get(ATTRIBUTE_NAME_PROPKEY);
      if (attrName == null) {
        throw new IllegalArgumentException(ATTRIBUTE_NAME_PROPKEY + " is not set");
      }
      Object baseTtl = config.get(BASE_TTL_PROPKEY);
      if (baseTtl == null) {
        throw new IllegalArgumentException(BASE_TTL_PROPKEY + " is not set");
      }
      return create((String) attrName, ((Number) baseTtl).longValue());
    }

    public TtlFormatter create(String attrName, long baseTtl) {
      if (baseTtl <= 0L) {
        throw new IllegalArgumentException("baseTtl must be grater than 0");
      }
      TtlFormatter elm = new TtlFormatter();
      elm.attrName = attrName;
      elm.baseTtl = baseTtl;
      return elm;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return TtlFormatter.class;
    }
  }
}
