package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;

public abstract class UnaryPrimitivePredicate implements PrimitivePredicate {

  protected final Expression operand;

  public UnaryPrimitivePredicate(Expression operand) {
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UnaryPrimitivePredicate that = (UnaryPrimitivePredicate) o;
    return Objects.equals(operand, that.operand);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operand);
  }
}
