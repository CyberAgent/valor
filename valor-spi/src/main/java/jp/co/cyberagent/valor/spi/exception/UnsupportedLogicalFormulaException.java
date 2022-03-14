package jp.co.cyberagent.valor.spi.exception;

import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.schema.Segment;

@SuppressWarnings("serial")
public class UnsupportedLogicalFormulaException extends Exception {

  public UnsupportedLogicalFormulaException(Segment schemaElement, PrimitivePredicate p) {
    super(p.getClass().getCanonicalName() + " is not supported in " + schemaElement.getClass()
        .getCanonicalName());
  }

  public UnsupportedLogicalFormulaException(String msg) {
    super(msg);
  }
}
