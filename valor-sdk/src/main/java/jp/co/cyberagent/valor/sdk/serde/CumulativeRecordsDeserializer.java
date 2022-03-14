package jp.co.cyberagent.valor.sdk.serde;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import jp.co.cyberagent.valor.sdk.formatter.CumulativeValueFormatter;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.util.TupleUtils;

public class CumulativeRecordsDeserializer<V> extends ContinuousRecordsDeserializer {
  private final CumulativeValueFormatter valueFormatter;
  private Map<String, Object> keys;
  private Map<String, Object> nextKeys;
  private Map<String, List<V>> values;
  private Map<String, List<V>> nextValues;

  public CumulativeRecordsDeserializer(Relation relation, String valueField,
                                       List<FieldLayout> layouts) {
    super(relation, layouts);
    if (valueField == null) {
      throw new IllegalArgumentException("value field name is not specified");
    }
    Optional<FieldLayout> valueLayout =
        layouts.stream().filter(f -> valueField.equals(f.getFieldName())).findFirst();
    if (valueField.isEmpty()) {
      throw new IllegalArgumentException("value layout is not found");
    }
    List<Segment> formatters = valueLayout.get().getFormatters();
    if (formatters.size() != 1) {
      throw new IllegalArgumentException(
          "the size of value field layouts expected to be 1 but " + formatters.size());
    }
    Formatter f = formatters.get(0).getFormatter();
    if (!(f instanceof CumulativeValueFormatter)) {
      throw new IllegalArgumentException("value formatter is not cumulative");
    }
    this.valueFormatter = (CumulativeValueFormatter) f;
  }

  @Override
  public void readRecord(List<String> fields, Record record) throws ValorException {
    nextKeys = new HashMap<>();
    nextValues = new HashMap<>();
    super.readRecord(fields, record);
    if (keys == null) {
      keys = nextKeys;
      values = nextValues;
      nextKeys = null;
      nextValues = null;
    } else {
      if (TupleUtils.haveSameKey(relation, keys, nextKeys)) {
        keys = TupleUtils.deepMerge(keys, nextKeys);
        values = mergeValueMap(values, nextValues);
        nextKeys = null;
        nextValues = null;
      } else {
        queue.add(buildTuple());
        keys = nextKeys;
        values = nextValues;
      }
    }
  }

  private Map<String, List<V>> mergeValueMap(Map<String, List<V>> m1, Map<String, List<V>> m2) {
    for (Map.Entry<String, List<V>> e2 : m2.entrySet()) {
      List<V> v1 = m1.get(e2.getKey());
      if (v1 == null) {
        v1 = new ArrayList<>();
        m1.put(e2.getKey(), v1);
      }
      v1.addAll(e2.getValue());
    }
    return m1;
  }

  private Tuple buildTuple() throws ValorException {
    if (keys == null) {
      return null;
    }
    Tuple tuple = new TupleImpl(relation);
    for (Map.Entry<String, Object> e : keys.entrySet()) {
      String attr = e.getKey();
      Object value = e.getValue();
      tuple.setAttribute(attr, value);
    }
    for (Map.Entry<String, List<V>> e : values.entrySet()) {
      Object value = valueFormatter.aggregate(e.getKey(), e.getValue());
      tuple.setAttribute(e.getKey(), value);
    }
    return tuple;
  }

  @Override
  public void putAttribute(String attr, byte[] in, int offset, int length) throws SerdeException {
    if (relation.isKey(attr)) {
      AttributeType type = getAttributeType(attr);
      nextKeys.put(attr, type.read(in, offset, length));
    } else {
      throw new IllegalArgumentException("unexpected byte array value for attribute " + attr);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void putAttribute(String attr, Object value) {
    if (relation.isKey(attr)) {
      nextKeys.put(attr, value);
    } else {
      List<V> v = nextValues.get(attr);
      if (v == null) {
        v = new ArrayList<>();
        nextValues.put(attr, v);
      }
      v.add((V) value);
    }
  }

  private AttributeType getAttributeType(String attr) {
    AttributeType type = relation.getAttributeType(attr);
    if (type == null) {
      throw new IllegalArgumentException(String.format("type of %s is not attribute of %", attr,
          relation.getRelationId()));
    }
    return type;
  }

  @Override
  public Tuple flushRemaining() throws ValorException {
    final Tuple t = buildTuple();
    keys = nextKeys;
    values = nextValues;
    nextKeys = null;
    nextValues = null;
    state.clear();
    return t;
  }
}
