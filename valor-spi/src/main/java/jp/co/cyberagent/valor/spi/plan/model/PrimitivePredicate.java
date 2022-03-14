package jp.co.cyberagent.valor.spi.plan.model;

import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;

public interface PrimitivePredicate extends PredicativeExpression {

  FilterSegment buildFilterFragment() throws SerdeException;

  PrimitivePredicate getNegation();

  @Override
  default OrOperator getDnf() {
    AndOperator conjunction = new AndOperator();
    conjunction.addOperand(this);
    OrOperator dnf = new OrOperator();
    dnf.addOperand(conjunction);
    return dnf;
  }

}
