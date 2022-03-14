package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.Relation;

public class RelationScan implements LogicalPlanNode {

  private static final ConstantExpression.BooleanConstantExpression DEFAULT_CONDITION
      = ConstantExpression.BooleanConstantExpression.TRUE;

  public static class Builder {
    ProjectionClause items;
    RelationSource source;
    PredicativeExpression condition;
    int limit = -1;

    public RelationScan.Builder setItems(Expression... e) {
      return setItems(Arrays.asList(e));
    }

    public RelationScan.Builder setItems(List<Expression> e) {
      return setItems(new ProjectionClause(e));
    }

    public RelationScan.Builder setItems(ProjectionClause pc) {
      this.items = pc;
      return this;
    }

    public RelationScan.Builder setRelationSource(String namespace, Relation relation) {
      return setRelationSource(new RelationSource(namespace, relation));
    }

    public RelationScan.Builder setRelationSource(RelationSource source) {
      this.source = source;
      return this;
    }

    public RelationScan.Builder setCondition(PredicativeExpression condition) {
      this.condition = condition;
      return this;
    }

    public RelationScan.Builder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    public RelationScan build() {
      if (this.items == null) {
        List<Expression> attrs =
            source.getRelation().getAttributes().stream()
                .map(a -> new AttributeNameExpression(a.name(), a.type()))
                .collect(Collectors.toList());
        this.items = new ProjectionClause(attrs);
      }
      return new RelationScan(items, source, condition, limit);
    }

  }

  private ProjectionClause projection;
  private RelationSource from;
  private PredicativeExpression where;
  private int limit;

  public RelationScan(
      ProjectionClause projection, RelationSource from, PredicativeExpression where) {
    this(projection, from, where, -1);
  }

  public RelationScan(
      ProjectionClause projection, RelationSource from, PredicativeExpression where, int limit) {
    this.projection = projection;
    this.from = from;
    this.where = where == null ? DEFAULT_CONDITION : where;
    this.limit = limit;
  }

  public List<ProjectionItem> getItems() {
    return projection.getItems();
  }

  public PredicativeExpression getCondition() {
    return where;
  }

  @Deprecated
  public ProjectionClause getProjection() {
    return projection;
  }

  public RelationSource getFrom() {
    return from;
  }

  public int getLimit() {
    return limit;
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      projection.accept(visitor);
      from.accept(visitor);
      if (where != null) {
        where.accept(visitor);
      }
    }
    visitor.leave(this);
  }

  @Override
  public String toString() {
    return String.format("%s %s %s", projection, from, where);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RelationScan query = (RelationScan) o;
    return Objects.equals(projection, query.projection)
        && Objects.equals(from, query.from)
        && Objects.equals(where, query.where)
        && Objects.equals(limit, limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(projection, from, where, limit);
  }
}
