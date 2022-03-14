package jp.co.cyberagent.valor.sdk.plan.function;

import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

/**
 * @deprecated use {@link RegexpNotMatchOperator}
 */
public class UnlikeOperator extends RegexpNotMatchOperator {

  public UnlikeOperator(Expression left, Expression right) {
    super(left, right);
  }

  public UnlikeOperator(String attr, AttributeType type, Object value) {
    super(attr, type, value);
  }

}
