package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;

@Deprecated
public class WhereClause implements LogicalPlanNode {

  private final PredicativeExpression predicate;

  public WhereClause(PredicativeExpression predicate) {
    this.predicate = predicate == null ? ConstantExpression.TRUE : predicate;
  }

  public PredicativeExpression getPredicate() {
    return predicate;
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      predicate.accept(visitor);
    }
    visitor.leave(this);
  }

  @Override
  public String toString() {
    return "WHERE " + predicate.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WhereClause)) {
      return false;
    }
    WhereClause that = (WhereClause) o;
    return Objects.equals(predicate, that.predicate);
  }

  @Override
  public int hashCode() {
    return Objects.hash(predicate);
  }
}
