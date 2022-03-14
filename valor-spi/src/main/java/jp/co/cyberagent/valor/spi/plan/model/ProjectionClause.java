package jp.co.cyberagent.valor.spi.plan.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.ImmutableAttribute;
import jp.co.cyberagent.valor.spi.relation.Relation;

public class ProjectionClause implements LogicalPlanNode {

  private final List<ProjectionItem> items;

  public ProjectionClause(Expression... items) {
    this(Arrays.asList(items));
  }

  public ProjectionClause(List<Expression> items) {
    this.items = new ArrayList<>(items.size());
    for (Expression e : items) {
      if (e instanceof AttributeNameExpression) {
        this.items.add(new ProjectionItem(e, ((AttributeNameExpression) e).getName()));
      } else {
        throw new UnsupportedOperationException(
            "only attribute expression is supported in projection, currently");
      }
    }
  }

  public List<ProjectionItem> getItems() {
    return items;
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      for (ProjectionItem item: items) {
        item.accept(visitor);
      }
    }
    visitor.leave(this);
  }

  public List<Relation.Attribute> getAttrs() {
    return items.stream()
        .map(e -> e.getValue())
        .filter(e -> e instanceof AttributeNameExpression)
        .map(AttributeNameExpression.class::cast)
        .map(a -> ImmutableAttribute.of(a.getName(), false, a.getType()))
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return "SELECT " + items.stream().map(i -> i.getAlias()).collect(Collectors.joining(", "));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ProjectionClause)) {
      return false;
    }
    ProjectionClause that = (ProjectionClause) o;
    return Objects.equals(items, that.items);
  }

  @Override
  public int hashCode() {
    return Objects.hash(items);
  }
}
