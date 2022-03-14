package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;

public class FilterMapFormatter extends Formatter {
  public static final String FORMATTER_TYPE = "filterMap";

  public static final String ATTRIBUTE_NAME_PROPKEY = "attr";

  public static final String INCLUDE_KEY = "include";
  private String attrName;
  private List<String> includeAttributes;

  public FilterMapFormatter() {
  }

  public FilterMapFormatter(Map<String, Object> conf) {
    setProperties(conf);
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    MapAttributeType type = (MapAttributeType) target.getRelation().getAttributeType(attrName);
    Map newMap = type.read(in, offset, length);
    Map currentMap = (Map) target.getAttribute(attrName);
    if (currentMap != null) {
      newMap.putAll(currentMap);
    }
    target.putAttribute(attrName, newMap);
    return length;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    Map v = filterMap((Map) tuple.getAttribute(attrName));
    MapAttributeType type = (MapAttributeType) tuple.getAttributeType(attrName);
    byte[] attributeAsBytes = type.serialize(v);
    serializer.write(null, attributeAsBytes);
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction) {
    // TODO implement filter
    serializer.write(null, TrueSegment.INSTANCE);
  }

  private Map filterMap(Map org) {
    if (includeAttributes != null) {
      return includeAttributes.stream().collect(Collectors.toMap(k -> k, k -> org.get(k)));
    } else {
      return org;
    }
  }

  @Override
  public boolean containsAttribute(String attr) {
    return attrName.equals(attr);
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> m = new HashMap<>();
    m.put(ATTRIBUTE_NAME_PROPKEY, attrName);
    if (includeAttributes != null) {
      m.put(INCLUDE_KEY, includeAttributes);
    }
    return m;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    attrName = (String) props.get(ATTRIBUTE_NAME_PROPKEY);
    if (props.containsKey(INCLUDE_KEY)) {
      includeAttributes = (List<String>) props.get(INCLUDE_KEY);
    }
  }

  public static class Factory implements FormatterFactory {
    @Override
    public Formatter create(Map config) {
      return new FilterMapFormatter(config);
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return FilterMapFormatter.class;
    }
  }
}
