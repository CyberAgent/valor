package jp.co.cyberagent.valor.sdk.serde;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.storage.Record;

/**
 * Deserializer for schemas including AggregationFormatters
 */
public class DisassembleDeserializer extends TupleDeserializerBase {

  public static final String UNNESTING_STATE_KEY = "unnest";
  public static final byte[] BEGIN_UNNESTING = {0x01};
  public static final byte[] COMMIT_UNNESTING = {0x02};

  private Map<String, Object> flatAttrs;
  private Map<String, Object> unnestedBuffer;

  private int pollIndex;
  private List<Map<String, Object>> unnestedValues = null;

  public DisassembleDeserializer(Relation relation, String fieldName, Segment... segments) {
    this(relation, Arrays.asList(FieldLayout.of(fieldName, segments)));
  }

  public DisassembleDeserializer(Relation relation, FieldLayout... formatters) {
    this(relation, Arrays.asList(formatters));
  }

  public DisassembleDeserializer(Relation relation, List<FieldLayout> formatters) {
    super(relation, formatters);
  }

  @Override
  public void readRecord(List<String> fields, Record record) throws ValorException {
    this.flatAttrs = new HashMap<>();
    this.pollIndex = 0;
    this.unnestedBuffer = null;
    this.unnestedValues = null;
    super.readRecord(fields, record);
  }

  @Override
  public void putAttribute(String attr, byte[] in, int offset, int length) throws SerdeException {
    AttributeType type = getAttributeType(attr);
    Object v = type.read(in, offset, length);
    putAttribute(attr, v);
  }

  @Override
  public void putAttribute(String attr, Object value) throws SerdeException {
    AttributeType type = getAttributeType(attr);
    if (!type.getRepresentedClass().isInstance(value)) {
      throw new IllegalArgumentException(
          type.toExpression() + " is expected but " + value.getClass());
    }
    if (unnestedBuffer != null) {
      unnestedBuffer.put(attr, value);
    } else {
      flatAttrs.put(attr, value);
    }
  }

  @Override
  public void setState(String name, byte[] value) {
    super.setState(name, value);
    if (UNNESTING_STATE_KEY.equals(name)) {
      if (BEGIN_UNNESTING[0] == value[0]) {
        if (unnestedBuffer != null) {
          throw new IllegalStateException("unnesting in unnested values is not supported");
        }
        unnestedBuffer = new HashMap<>();
      } else if (COMMIT_UNNESTING[0] == value[0]) {
        if (unnestedBuffer == null) {
          throw new IllegalStateException("unnesting have not started");
        }
        if (unnestedValues == null) {
          unnestedValues = new ArrayList<>();
        }
        unnestedValues.add(unnestedBuffer);
        unnestedBuffer = null;
      }
    }
  }

  @Override
  public Object getAttribute(String attr) {
    return flatAttrs.get(attr);
  }

  private AttributeType getAttributeType(String attr) {
    AttributeType type = relation.getAttribute(attr).type();
    if (type == null) {
      throw new IllegalArgumentException(String.format("type of %s is not attribute of %", attr,
          relation.getRelationId()));
    }
    return type;
  }

  @Override
  public Tuple pollTuple() {
    if (flatAttrs == null) {
      // no record is not read yet
      return null;
    }

    if (unnestedValues != null && unnestedValues.isEmpty()) {
      // no unnested values and no iteration
      return null;
    }
    Tuple t = new TupleImpl(relation);
    for (Map.Entry<String, Object> fe : flatAttrs.entrySet()) {
      t.setAttribute(fe.getKey(), fe.getValue());
    }
    // currently only support one nested attribute
    if (unnestedValues != null) {
      if (pollIndex >= unnestedValues.size()) {
        return null;
      }
      Map<String, Object> unnestedAttrs = unnestedValues.get(pollIndex++);
      for (Map.Entry<String, Object> e : unnestedAttrs.entrySet()) {
        t.setAttribute(e.getKey(), e.getValue());
      }
    } else {
      // no nested attribute, return current tuple and reset record contents
      flatAttrs = null;
      return t;
    }
    return t;
  }
}
