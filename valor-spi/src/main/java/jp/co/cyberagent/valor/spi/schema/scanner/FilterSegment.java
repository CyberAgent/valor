package jp.co.cyberagent.valor.spi.schema.scanner;

import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;

public interface FilterSegment {

  enum Type {
    TRUE,
    FALSE,
    BETWEEN,
    // single value fragment types from here
    COMPLETE_MATCH,
    LESS_THAN,
    GREATER_THAN,
    REGEXP,
    NOT_MATCH
  }

  Type type();

  default PredicativeExpression getOrigin() {
    return null;
  }

  FilterSegment mergeByAnd(FilterSegment otherFragment);

  boolean evaluate(byte[] value);
}
