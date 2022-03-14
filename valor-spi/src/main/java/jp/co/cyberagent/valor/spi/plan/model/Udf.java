package jp.co.cyberagent.valor.spi.plan.model;

import java.util.List;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

public interface Udf<V> {

  String getName();

  void init(List<Expression> argExps);

  V apply(Object... args);

  AttributeType<V> getReturnType();

}
