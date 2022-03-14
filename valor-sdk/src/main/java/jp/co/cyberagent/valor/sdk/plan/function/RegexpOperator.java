package jp.co.cyberagent.valor.sdk.plan.function;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.RegexpMatchSegment;

public class RegexpOperator extends BinaryPrimitivePredicate {

  private Pattern pattern;
  private Map<String, Pattern> mapPattern;

  public RegexpOperator(Expression left, Expression right) {
    super(left, right);
    if (right instanceof ConstantExpression) {
      initPattern(((ConstantExpression<?>) right).getValue());
    }
  }

  public RegexpOperator(String attr, AttributeType type, Object value) {
    super(attr, type, value);
  }

  private void initPattern(Object value) {
    if (value instanceof String) {
      pattern = Pattern.compile((String) value);
    } else if (value instanceof Map) {
      mapPattern = new HashMap<>();
      for (Map.Entry e : ((Map<?, ?>) value).entrySet()) {
        if (!(e.getKey() instanceof String)) {
          throw new IllegalArgumentException("unexpected type key " + e.getKey());
        }
        if (!(e.getValue() instanceof String)) {
          throw new IllegalArgumentException("unexpected type value " + e.getValue());
        }
        mapPattern.put((String) e.getKey(), Pattern.compile((String) e.getValue()));
      }
    }
  }

  @Override
  public Boolean apply(Tuple tuple) {
    Object lv = left.apply(tuple);
    Object rv = right.apply(tuple);
    if (lv == null) {
      return null;
    }
    return test(lv, rv);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected boolean test(Object lv, Object rv) {
    if (lv == null) {
      return false;
    }
    // TODO cache pattern
    if (lv instanceof String) {
      Pattern p = this.pattern != null ? this.pattern : Pattern.compile((String) rv);
      return p.matcher((String) lv).matches();
    } else if (lv instanceof Map) {
      Map mv = (Map) lv;
      if (mapPattern != null) {
        return testByMapPattern(mv);
      } else {
        throw new UnsupportedOperationException("map pattern has to be specified by a constant");
      }
    }
    throw new IllegalArgumentException("unexpected type " + lv);
  }

  private boolean testByMapPattern(Map mv) {
    if (mapPattern.size() != mv.size()) {
      return false;
    }
    for (Map.Entry<String, Pattern> pe : mapPattern.entrySet()) {
      Object v = mv.get(pe.getKey());
      if (v == null) {
        return false;
      }
      if (!(v instanceof String)) {
        throw new IllegalArgumentException("unexpected type value " + v);
      }
      boolean r = pe.getValue().matcher((String) v).matches();
      if (!r) {
        return false;
      }
    }
    return true;
  }

  @Override
  protected FilterSegment buildAttributeCompareFragment(AttributeNameExpression attr,
                                                        ConstantExpression value,
                                                        boolean swapped) throws SerdeException {
    return new RegexpMatchSegment(this, attr.getType().serialize(value.getValue()), true);
  }

  @Override
  protected String getOperatorExpression() {
    return "LIKE";
  }

  @Override
  public PrimitivePredicate getNegation() {
    return new RegexpNotMatchOperator(left, right);
  }
}
