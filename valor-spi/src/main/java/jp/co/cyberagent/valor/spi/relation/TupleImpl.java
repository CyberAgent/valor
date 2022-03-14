package jp.co.cyberagent.valor.spi.relation;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class TupleImpl implements Tuple {

  protected Relation relation;

  protected Map<String, Object> attributes = new HashMap<String, Object>();

  public TupleImpl(Relation relation) {
    this.relation = relation;
  }

  @Override
  public Object getAttribute(String attr) {
    return attributes.get(attr);
  }

  @Override
  public AttributeType getAttributeType(String attr) {
    return relation.getAttributeType(attr);
  }

  @Override
  public Collection<String> getAttributeNames() {
    return this.attributes.keySet();
  }

  @Override
  public void setAttribute(String attr, Object value) {
    this.attributes.put(attr, value);
  }

  @Override
  public String toString() {
    StringBuilder buf = null;
    for (Map.Entry<String, Object> attr : attributes.entrySet()) {
      String key = attr.getKey();
      Object value = attr.getValue();
      if (value instanceof byte[]) {
        value = ByteUtils.toStringBinary((byte[]) value);
      }
      if (buf == null) {
        buf = new StringBuilder();
        buf.append("[");
        buf.append(key);
        buf.append("=");
        buf.append(value);
      } else {
        buf.append(",");
        buf.append(key);
        buf.append("=");
        buf.append(value);
      }
    }
    if (buf == null) {
      return "[]";
    }
    buf.append("]");
    return buf.toString();
  }

  @Override
  public Tuple getCopy() {
    TupleImpl copy = new TupleImpl(relation);
    for (Map.Entry<String, Object> attr : this.attributes.entrySet()) {
      copy.setAttribute(attr.getKey(), attr.getValue());
    }
    return copy;
  }
}
