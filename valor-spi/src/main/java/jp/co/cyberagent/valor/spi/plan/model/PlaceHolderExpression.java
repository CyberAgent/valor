package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

public class PlaceHolderExpression implements Expression<Object> {

  protected Object value;

  @Override
  public AttributeType getType() {
    return AttributeType.getTypeOf(value);
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    visitor.visit(this);
    visitor.leave(this);
  }

  @Override
  public Object apply(Tuple tuple) {
    return value;
  }

  @Override
  public String toString() {
    return value == null ? "NULL" : value.toString();
  }

  public Object getValue() {
    return value;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PlaceHolderExpression that = (PlaceHolderExpression) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
