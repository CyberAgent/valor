package jp.co.cyberagent.valor.spi.plan.model;

import java.util.function.Function;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

public interface Expression<T> extends LogicalPlanNode, Function<Tuple, T> {

  AttributeType getType();

}
