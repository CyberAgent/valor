package jp.co.cyberagent.valor.spi.plan.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;

public class ValueItem implements LogicalPlanNode {

  List<ConstantExpression> values = new ArrayList<>();

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    visitor.visit(this);
    visitor.leave(this);
  }

  public List<ConstantExpression> getValues() {
    return values;
  }

  public void addValue(ConstantExpression v) {
    this.values.add(v);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValueItem valueItem = (ValueItem) o;
    return Objects.equals(values, valueItem.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

}
