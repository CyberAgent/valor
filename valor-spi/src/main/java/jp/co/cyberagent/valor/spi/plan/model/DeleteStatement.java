package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;

public class DeleteStatement implements LogicalPlanNode {

  private RelationSource relation;

  private PredicativeExpression condition;

  private int limit;

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      relation.accept(visitor);
      condition.accept(visitor);
    }
    visitor.leave(this);
  }

  public RelationSource getRelation() {
    return relation;
  }

  public void setRelation(RelationSource relation) {
    this.relation = relation;
  }

  public PredicativeExpression getCondition() {
    return condition;
  }

  public void setCondition(PredicativeExpression condition) {
    this.condition = condition;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DeleteStatement that = (DeleteStatement) o;
    return Objects.equals(relation, that.relation)
        && Objects.equals(condition, that.condition)
        && Objects.equals(limit, this.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relation, condition, limit);
  }


}
