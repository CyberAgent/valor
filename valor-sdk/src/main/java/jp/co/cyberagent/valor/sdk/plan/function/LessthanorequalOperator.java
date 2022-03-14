package jp.co.cyberagent.valor.sdk.plan.function;

import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;

public class LessthanorequalOperator extends BinaryPrimitivePredicate {

  public LessthanorequalOperator(Expression left, Expression right) {
    super(left, right);
  }

  public LessthanorequalOperator(String attr, AttributeType type, Object value) {
    super(attr, type, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected boolean test(Object lv, Object rv) {
    Comparable leftComparable = (Comparable) lv;
    int comparition = leftComparable.compareTo(rv);
    return comparition <= 0;
  }

  @Override
  protected FilterSegment buildAttributeCompareFragment(AttributeNameExpression attr,
                                                        ConstantExpression value,
                                                        boolean swapped) throws SerdeException {
    byte[] val = attr.getType().serialize(value.getValue());
    return swapped
        ? new GreaterThanSegment(this, val, true)
        : new LessThanSegment(this, val, true);
  }

  @Override
  protected String getOperatorExpression() {
    return "<=";
  }

  @Override
  public PrimitivePredicate getNegation() {
    return new GreaterthanOperator(left, right);
  }
}
