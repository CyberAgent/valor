package jp.co.cyberagent.valor.sdk.plan.function;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.NotMatchSegment;

// TODO remove (replace with not(equal)
public class NotEqualOperator extends BinaryPrimitivePredicate {

  public NotEqualOperator(Expression left, Expression right) {
    super(left, right);
  }

  public NotEqualOperator(String attr, AttributeType type, Object value) {
    super(attr, type, value);
  }

  @Override
  protected boolean test(Object lv, Object rv) {
    return !Objects.equals(lv, rv);
  }

  @Override
  protected FilterSegment buildAttributeCompareFragment(AttributeNameExpression attr,
                                                        ConstantExpression value,
                                                        boolean swapped) throws SerdeException {
    return new NotMatchSegment(this, attr.getType().serialize(value.getValue()));
  }

  @Override
  protected String getOperatorExpression() {
    return "!=";
  }

  @Override
  public PrimitivePredicate getNegation() {
    return new EqualOperator(left, right);
  }
}
