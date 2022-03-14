package jp.co.cyberagent.valor.spi.plan.model;

import java.util.List;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;

public class ParameterizedStatement implements LogicalPlanNode {

  private final LogicalPlanNode statement;

  private final List<PlaceHolderExpression> parameters;

  public ParameterizedStatement(LogicalPlanNode statement, List<PlaceHolderExpression> parameters) {
    this.statement = statement;
    this.parameters = parameters;
  }

  public void setParameter(int index, Object value) {
    this.parameters.get(index).setValue(value);
  }

  public void clearParameter() {
    this.parameters.forEach(v -> v.setValue(null));
  }

  public LogicalPlanNode getStatement() {
    return statement;
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    statement.accept(visitor);
  }
}
