package jp.co.cyberagent.valor.sdk.serde;

import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.storage.Record;

public class OneToOneDeserializer extends TupleDeserializerBase {

  private Tuple tuple;

  public OneToOneDeserializer(Relation relation, String fieldName, Segment... segments) {
    this(relation, Arrays.asList(FieldLayout.of(fieldName, segments)));
  }

  public OneToOneDeserializer(Relation relation, FieldLayout... formatters) {
    this(relation, Arrays.asList(formatters));
  }

  public OneToOneDeserializer(Relation relation, List<FieldLayout> formatters) {
    super(relation, formatters);
  }

  @Override
  public void readRecord(List<String> fields, Record record) throws ValorException {
    tuple = new TupleImpl(relation);
    super.readRecord(fields, record);
  }

  @Override
  public void putAttribute(String attr, byte[] in, int offset, int length) throws SerdeException {
    AttributeType type = getAttributeType(attr);
    Object v = type.read(in, offset, length);
    tuple.setAttribute(attr, v);
  }

  @Override
  public void putAttribute(String attr, Object value) throws SerdeException {
    AttributeType type = getAttributeType(attr);
    if (!type.getRepresentedClass().isInstance(value)) {
      throw new IllegalArgumentException(
          type.toExpression() + " is expected but " + value.getClass());
    }
    tuple.setAttribute(attr, value);
  }

  @Override
  public Object getAttribute(String attr) {
    return tuple.getAttribute(attr);
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
  public Tuple pollTuple() {
    Tuple t = tuple;
    tuple = null;
    return t;
  }
}
