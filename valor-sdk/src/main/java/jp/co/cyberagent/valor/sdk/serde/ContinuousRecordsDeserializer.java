package jp.co.cyberagent.valor.sdk.serde;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.util.TupleUtils;

public class ContinuousRecordsDeserializer extends TupleDeserializerBase {
  public static final String ATTR_NAME_STATE_KEY = "attrName";
  public static final String MAP_KEY_STATE_KEY = "mapKeyName";
  public static final String MAP_VALUE_STATE_KEY = "mapValue";

  protected Queue<Tuple> queue = new LinkedList<>();
  private Tuple prevTuple;
  private Tuple nextTuple;

  public ContinuousRecordsDeserializer(Relation relation, List<FieldLayout> formatters) {
    super(relation, formatters);
  }

  @Override
  public void readRecord(List<String> fields, Record record) throws ValorException {
    nextTuple = new TupleImpl(relation);
    super.readRecord(fields, record);
    if (prevTuple == null) {
      prevTuple = nextTuple;
    } else if (haveSameKey(prevTuple, nextTuple)) {
      prevTuple = TupleUtils.deepMerge(prevTuple, nextTuple);
    } else {
      queue.add(prevTuple);
      prevTuple = nextTuple;
      nextTuple = null;
    }
  }

  private boolean haveSameKey(Tuple t1, Tuple t2) {
    for (String attr : relation.getKeyAttributeNames()) {
      if (!Objects.equals(t2.getAttribute(attr), t1.getAttribute(attr))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void putAttribute(String attr, byte[] in, int offset, int length) throws SerdeException {
    Relation.Attribute a = relation.getAttribute(attr);
    if (a == null) {
      throw new IllegalArgumentException(String.format("%s is not attribute of %s", attr,
          relation.getRelationId()));
    }
    this.putAttribute(attr, a.type().read(in, offset, length));
  }

  @Override
  public void putAttribute(String attr, Object value) throws SerdeException {
    nextTuple.setAttribute(attr, value);
  }

  @Override
  public Object getAttribute(String attr) {
    return nextTuple.getAttribute(attr);
  }

  @Override
  public Tuple pollTuple() {
    return queue.poll();
  }

  public Tuple flushRemaining() throws ValorException {
    Tuple t = prevTuple;
    prevTuple = null;
    return t;
  }
}
