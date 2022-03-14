package jp.co.cyberagent.valor.spi.plan.model;

import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;

public interface LogicalPlanNode {

  void accept(LogicalPlanVisitor visitor);

}
