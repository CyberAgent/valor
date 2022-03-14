package jp.co.cyberagent.valor.sdk.plan.function;

import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.UnaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.IsnullSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;

public class IsNullOperator extends UnaryPrimitivePredicate {

  public IsNullOperator(Expression operand) {
    super(operand);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Boolean apply(Tuple tuple) {
    Object v = operand.apply(tuple);
    return v == null;
  }

  @Override
  public String toString() {
    return String.format("%s IS NULL", operand == null ? "null" : operand.toString());
  }

  @Override
  public FilterSegment buildFilterFragment() throws SerdeException {
    if (operand instanceof AttributeNameExpression) {
      return new IsnullSegment(this);
    } else {
      return TrueSegment.INSTANCE;
    }
  }

  @Override
  public PrimitivePredicate getNegation() {
    return new IsNotNullOperator(operand);
  }
}
