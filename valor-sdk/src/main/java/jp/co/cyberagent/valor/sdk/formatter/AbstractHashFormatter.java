package jp.co.cyberagent.valor.sdk.formatter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;

public abstract class AbstractHashFormatter extends Formatter {

  public static final String ATTRIBUTE_NAME_PROPKEY = "attr";
  public static final String ATTRIBUTES_NAME_PROPKEY = "attrs";
  public static final String LENGTH_PROPKEY = "length";

  protected int length;

  protected List<String> attrNames;

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    return this.length;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    byte[] fragmentValue;
    if (attrNames.size() == 1) {
      String attr = attrNames.get(0);
      AttributeType type = tuple.getAttributeType(attr);
      fragmentValue = type.serialize(tuple.getAttribute(attr));
    } else {
      List<byte[]> fragmentValues = new ArrayList<>(attrNames.size());
      int totalLength = 0;
      for (String attrName: attrNames) {
        AttributeType type = tuple.getAttributeType(attrName);
        byte[] value = type.serialize(tuple.getAttribute(attrName));
        fragmentValues.add(value);
        totalLength += value.length;
      }
      ByteBuffer buf = ByteBuffer.allocate(totalLength);
      fragmentValues.stream().forEach(buf::put);
      fragmentValue = buf.array();
    }
    fragmentValue = toHashBytes(fragmentValue);
    serializer.write(null, fragmentValue);
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws SerdeException {
    List<String> attrs = new ArrayList(attrNames);
    Map<String, EqualOperator> predicates = new HashMap<>();
    for (PrimitivePredicate cond : conjunction) {
      if (!(cond instanceof EqualOperator)) {
        continue;
      }
      EqualOperator eq = (EqualOperator)cond;
      String operandAttr = eq.getAttributeIfUnaryPredicate();
      if (attrs.contains(operandAttr)) {
        predicates.put(operandAttr, eq);
        attrs.remove(operandAttr);
      }
    }
    if (predicates.isEmpty() || !attrs.isEmpty()) {
      serializer.write(null, TrueSegment.INSTANCE);
    } else {
      List<byte[]> fragmentValues = new ArrayList<>(attrNames.size());
      int totalLength = 0;
      for (String attr : attrNames) {
        FilterSegment f = predicates.get(attr).buildFilterFragment();
        byte[] v = ((CompleteMatchSegment)f).getValue();
        fragmentValues.add(v);
        totalLength += v.length;
      }
      ByteBuffer buf = ByteBuffer.allocate(totalLength);
      fragmentValues.stream().forEach(buf::put);
      byte[] hashValue = toHashBytes(buf.array());
      serializer.write(null, new CompleteMatchSegment(null, hashValue));
    }
  }

  @Override
  public boolean containsAttribute(String attr) {
    return false;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(LENGTH_PROPKEY, length);
    props.put(ATTRIBUTES_NAME_PROPKEY, attrNames);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    if (props.containsKey(LENGTH_PROPKEY)) {
      length = (int) props.get(LENGTH_PROPKEY);
      if (length < 1 || length > getMaxLength()) {
        throw new IllegalArgumentException(LENGTH_PROPKEY + " is out of range");
      }
    } else {
      length = getMaxLength();
    }

    if (props.containsKey(ATTRIBUTES_NAME_PROPKEY)) {
      this.attrNames = (List) props.get(ATTRIBUTES_NAME_PROPKEY);
    } else {
      Object attr = props.get(ATTRIBUTE_NAME_PROPKEY);
      if (attr == null || !(attr instanceof String)) {
        throw new IllegalArgumentException(
            attr == null ? "attr is not specified" : attr + " is not a string");
      }
      this.attrNames = Arrays.asList((String)attr);
    }
  }

  protected abstract int getMaxLength();

  protected byte[] toHashBytes(byte[] value) {
    byte[] h = calculateHash(value);
    return Arrays.copyOf(h, length);
  }

  protected abstract byte[] calculateHash(byte[] value);

}
