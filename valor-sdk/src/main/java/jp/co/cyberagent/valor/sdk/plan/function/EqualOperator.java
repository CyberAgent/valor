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
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;

public class EqualOperator extends BinaryPrimitivePredicate {
  public EqualOperator(Expression left, Expression right) {
    super(left, right);
  }

  /**
   * alias to {@code new EqualOperator(new AttriubteNameExpression(attr, type), new
   * ConstantExpression(value))}
   */
  public EqualOperator(String attr, AttributeType type, Object value) {
    super(attr, type, value);
  }

  @Override
  protected boolean test(Object lv, Object rv) {
    return Objects.equals(lv, rv);
  }

  @Override
  protected FilterSegment buildAttributeCompareFragment(AttributeNameExpression attr,
                                                        ConstantExpression value,
                                                        boolean swapped) throws SerdeException {
    return new CompleteMatchSegment(this, attr.getType().serialize(value.getValue()));
  }

  @Override
  protected String getOperatorExpression() {
    return "=";
  }

  @Override
  public PrimitivePredicate getNegation() {
    return new NotEqualOperator(left, right);
  }
}
