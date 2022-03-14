package jp.co.cyberagent.valor.sdk.plan.function;

import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

/**
 *  @deprecated use {@link RegexpOperator}
 */
@Deprecated
public class LikeOperator extends RegexpOperator {

  public LikeOperator(Expression left, Expression right) {
    super(left, right);
  }

  public LikeOperator(String attr, AttributeType type, Object value) {
    super(attr, type, value);
  }

}
