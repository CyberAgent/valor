package jp.co.cyberagent.valor.sdk.plan.function;

import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.UnaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;

public class IsNotNullOperator extends UnaryPrimitivePredicate {
  public IsNotNullOperator(Expression operand) {
    super(operand);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Boolean apply(Tuple tuple) {
    Object v = operand.apply(tuple);
    return v != null;
  }

  @Override
  public String toString() {
    return String.format("%s IS NOT NULL", operand == null ? "null" : operand.toString());
  }

  @Override
  public FilterSegment buildFilterFragment() throws SerdeException {
    // cannot handle in scan
    return TrueSegment.INSTANCE;
  }

  @Override
  public PrimitivePredicate getNegation() {
    return new IsNullOperator(operand);
  }
}
