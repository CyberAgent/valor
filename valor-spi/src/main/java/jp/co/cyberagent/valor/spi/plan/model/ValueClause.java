package jp.co.cyberagent.valor.spi.plan.model;

import java.util.ArrayList;
import java.util.List;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;

public class ValueClause implements LogicalPlanNode {

  private List<ValueItem> items = new ArrayList<>();

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      items.forEach(i -> i.accept(visitor));
    }
    visitor.leave(this);
  }

  public List<ValueItem> getItems() {
    return items;
  }

  public void addItem(ValueItem item) {
    this.items.add(item);
  }
}
