package jp.co.cyberagent.valor.spi.plan.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.spi.relation.Relation;

/**
 * @deprecated replaced {@link RelationScan}
 */
@Deprecated
public class Query extends RelationScan {

  public static class SimpleBuilder {
    List<Expression> items = new ArrayList<>();
    RelationSource source;
    WhereClause condition;

    public SimpleBuilder addItems(Expression... e) {
      Arrays.stream(e).forEach(items::add);
      return this;
    }

    public SimpleBuilder addItems(List<Expression> e) {
      e.stream().forEach(items::add);
      return this;
    }

    public SimpleBuilder setRelationName(String namespace, Relation relation) {
      this.source = new RelationSource(namespace, relation);
      return this;
    }

    public SimpleBuilder setCondition(PredicativeExpression condition) {
      this.condition = new WhereClause(condition);
      return this;
    }

    public Query build() {
      if (items.isEmpty()) {
        source.getRelation().getAttributes().forEach(a ->
            items.add(new AttributeNameExpression(a.name(), a.type())));
      }
      return new Query(new ProjectionClause(items), source, condition);
    }
  }

  public static SimpleBuilder builder() {
    return new SimpleBuilder();
  }

  public Query(ProjectionClause projection, FromClause from, WhereClause where) {
    this(projection, from, where, -1);
  }

  public Query(ProjectionClause projection, FromClause from, WhereClause where, int limit) {
    super(projection, (RelationSource) from, where.getPredicate(), limit);
  }

}
