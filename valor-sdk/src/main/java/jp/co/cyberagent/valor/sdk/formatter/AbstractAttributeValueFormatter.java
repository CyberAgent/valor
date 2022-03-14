package jp.co.cyberagent.valor.sdk.formatter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.UnaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;

/**
 * a schema element representing a value of a single attribute
 */
public abstract class AbstractAttributeValueFormatter extends Formatter {
  public static final String ATTRIBUTE_NAME_PROPKEY = "attr";

  protected String attrName;

  public AbstractAttributeValueFormatter() {
  }

  public AbstractAttributeValueFormatter(String attrName) {
    this.attrName = attrName;
  }

  @Override
  public String toString() {
    return String.format("%s['%s']", getName(), attrName);
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(ATTRIBUTE_NAME_PROPKEY, attrName);
    return props;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    this.attrName = (String) props.get(ATTRIBUTE_NAME_PROPKEY);
  }

  @Override
  public boolean containsAttribute(String attr) {
    if (attr == null) {
      return false;
    }
    return attr.equals(attrName);
  }

  public String getAttributeName() {
    return attrName;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws SerdeException {
    AttributeType type = tuple.getAttributeType(attrName);
    byte[] attributeAsBytes = serialize(tuple.getAttribute(attrName), type);
    serializer.write(null, attributeAsBytes);
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws SerdeException {
    List<PrimitivePredicate> predicates = filterByAttrName(conjunction);
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

  protected abstract byte[] serialize(Object attrVal, AttributeType<?> type);

  protected abstract FilterSegment convert(FilterSegment fragment);

  protected List<PrimitivePredicate> filterByAttrName(Collection<PrimitivePredicate> conjunction) {
    List<PrimitivePredicate> filtered = new ArrayList<>();
    for (PrimitivePredicate predicate : conjunction) {
      if (predicate instanceof BinaryPrimitivePredicate) {
        BinaryPrimitivePredicate bop = (BinaryPrimitivePredicate) predicate;
        if (attrName.equals(bop.getAttributeIfUnaryPredicate())) {
          filtered.add(bop);
        }
      } else if (predicate instanceof UnaryPrimitivePredicate) {
        UnaryPrimitivePredicate uop = (UnaryPrimitivePredicate) predicate;
        Expression operand = uop.getOperand();
        if (operand instanceof AttributeNameExpression) {
          if (attrName.equals(((AttributeNameExpression)operand).getName())) {
            filtered.add(uop);
          }
        }
      }
    }
    return filtered;
  }
}
