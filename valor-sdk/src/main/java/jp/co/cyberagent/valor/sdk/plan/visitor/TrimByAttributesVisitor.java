package jp.co.cyberagent.valor.sdk.plan.visitor;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.FromClause;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionClause;
import jp.co.cyberagent.valor.spi.plan.model.Query;
import jp.co.cyberagent.valor.spi.plan.model.RelationSource;
import jp.co.cyberagent.valor.spi.plan.model.WhereClause;


public class TrimByAttributesVisitor extends LogicalPlanVisitorBase<LogicalPlanNode> {

  public static <T extends LogicalPlanNode> T trim(T exp, Collection<String> attributes) {
    TrimByAttributesVisitor visitor = new TrimByAttributesVisitor(attributes);
    return (T) visitor.walk(exp);
  }

  private final Collection<String> attributes;

  public TrimByAttributesVisitor(Collection<String> attributes) {
    this.attributes = attributes;
  }

  protected void leaveQuery(Query e) {
    List<LogicalPlanNode> child = childResult.peek();
    ProjectionClause projection = (ProjectionClause) child.get(0);
    FromClause from = (FromClause) child.get(1);
    WhereClause where = (WhereClause) child.get(2);
    this.result = new Query(projection, from, where);
  }

  protected void leaveProjectionClause(ProjectionClause e) {
    List<LogicalPlanNode> child = childResult.peek();
    List<Expression> items = child.stream()
        .filter(i -> i != null)
        .map(Expression.class::cast)
        .collect(Collectors.toList());
    this.result = new ProjectionClause(items);
  }

  protected void leaveFromClause(FromClause e) {
    if (!(e instanceof RelationSource)) {
      throw new UnsupportedOperationException("unsupported source " + e);
    }
    this.result = e;
  }

  protected void leaveWhereClause(WhereClause e) {
    PredicativeExpression predicate = (PredicativeExpression) this.childResult.peek().get(0);
    this.result = new WhereClause(predicate);
  }

  protected void leaveConstantExpression(ConstantExpression e) {
    this.result = e;
  }

  protected void leaveAttributeNameExpression(AttributeNameExpression e) {
    this.result = attributes.contains(e.getName()) ? e : null;
  }

  protected void leaveAndOperator(AndOperator e) {
    List<LogicalPlanNode> child = childResult.peek();
    LogicalPlanNode left = child.get(0);
    LogicalPlanNode right = child.get(1);
    if (left != null && right != null) {
      this.result = e;
    } else if (left == null && right == null) {
      this.result = ConstantExpression.TRUE;
    } else {
      this.result = left == null ? right : left;
    }
  }

  protected void leaveOrOperator(OrOperator e) {
    List<LogicalPlanNode> child = childResult.peek();
    LogicalPlanNode left = child.get(0);
    LogicalPlanNode right = child.get(1);
    if (left != null && right != null) {
      this.result = e;
    } else {
      this.result = ConstantExpression.TRUE;
    }
  }

  protected void leaveBinaryPrimitivePredicate(BinaryPrimitivePredicate e) {
    List<LogicalPlanNode> child = childResult.peek();
    LogicalPlanNode left = child.get(0);
    LogicalPlanNode right = child.get(1);
    if (left != null && right != null) {
      this.result = e;
    } else {
      this.result = null;
    }
  }
}
