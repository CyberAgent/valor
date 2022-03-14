package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

public class ProjectionItem implements Expression<Object> {

  private Expression value;

  private String alias;

  public ProjectionItem(Expression value) {
    this(value, null);
  }

  public ProjectionItem(Expression value, String alias) {
    this.value = value;
    this.alias = alias;
  }

  @Override
  public AttributeType getType() {
    return value.getType();
  }

  @Override
  public Object apply(Tuple o) {
    return value.apply(o);
  }

  public Expression getValue() {
    return value;
  }

  public String getAlias() {
    return alias == null ? value.toString() : alias;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProjectionItem that = (ProjectionItem) o;
    return Objects.equals(value, that.value) && Objects.equals(alias, that.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value, alias);
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      value.accept(visitor);
    }
    visitor.leave(this);
  }

}
