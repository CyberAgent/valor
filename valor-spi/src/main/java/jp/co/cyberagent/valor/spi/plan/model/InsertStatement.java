package jp.co.cyberagent.valor.spi.plan.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;

public class InsertStatement implements LogicalPlanNode {

  private RelationSource relation;

  private List<String> attributes;

  private List<ValueItem> values = new ArrayList<>();

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      relation.accept(visitor);
    }
    visitor.leave(this);
  }

  public RelationSource getRelation() {
    return relation;
  }

  public void setRelation(RelationSource relation) {
    this.relation = relation;
  }

  public List<String> getAttributes() {
    return attributes;
  }

  public void setAttributes(List<String> attributes) {
    this.attributes = attributes;
  }

  public List<ValueItem> getValues() {
    return values;
  }

  public void addValue(ValueItem value) {
    this.values.add(value);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InsertStatement that = (InsertStatement) o;
    return Objects.equals(relation, that.relation)
        && Objects.equals(attributes, that.attributes)
        && Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relation, attributes, values);
  }

}
