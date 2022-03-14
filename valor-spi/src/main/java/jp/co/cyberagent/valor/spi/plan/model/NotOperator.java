package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.Tuple;

public class NotOperator implements PredicativeExpression {

  protected final PredicativeExpression operand;

  public NotOperator(PredicativeExpression operand) {
    this.operand = operand;
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      operand.accept(visitor);
    }
    visitor.leave(this);
  }

  public Expression getOperand() {
    return this.operand;
  }

  @Override
  public OrOperator getDnf() {
    return PredicativeExpression.getNegation(this.operand.getDnf());
  }

  @SuppressWarnings("unchecked")
  @Override
  public Boolean apply(Tuple tuple) {
    return operand.negate().test(tuple);
  }

  @Override
  public String toString() {
    return String.format("NOT %s", operand == null ? "null" : operand.toString());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NotOperator)) {
      return false;
    }
    NotOperator that = (NotOperator) o;
    return Objects.equals(operand, that.operand);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operand);
  }
}
