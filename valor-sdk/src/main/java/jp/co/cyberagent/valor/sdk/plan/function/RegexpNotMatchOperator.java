package jp.co.cyberagent.valor.sdk.plan.function;

import java.util.regex.Pattern;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;

public class RegexpNotMatchOperator extends BinaryPrimitivePredicate {

  public RegexpNotMatchOperator(Expression left, Expression right) {
    super(left, right);
  }

  public RegexpNotMatchOperator(String attr, AttributeType type, Object value) {
    super(attr, type, value);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected boolean test(Object lv, Object rv) {
    // TODO cache pattern
    Pattern pattern = Pattern.compile((String) rv);
    return !pattern.matcher((String) lv).matches();
  }

  @Override
  protected FilterSegment buildAttributeCompareFragment(AttributeNameExpression attr,
                                                        ConstantExpression value,
                                                        boolean swapped) throws SerdeException {
    // not filter storage level
    return TrueSegment.INSTANCE;
  }

  @Override
  protected String getOperatorExpression() {
    return "UNLIKE";
  }

  @Override
  public PrimitivePredicate getNegation() {
    return new RegexpOperator(left, right);
  }
}
