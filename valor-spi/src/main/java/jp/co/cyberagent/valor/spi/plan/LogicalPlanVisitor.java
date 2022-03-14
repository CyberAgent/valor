package jp.co.cyberagent.valor.spi.plan;

import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;

/**
 *
 */
public interface LogicalPlanVisitor<T> {

  boolean visit(LogicalPlanNode node);

  void leave(LogicalPlanNode node);

}
