package jp.co.cyberagent.valor.cli.ast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.IsNotNullOperator;
import jp.co.cyberagent.valor.sdk.plan.function.IsNullOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.NotEqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.RegexpNotMatchOperator;
import jp.co.cyberagent.valor.sdk.plan.function.RegexpOperator;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;
import jp.co.cyberagent.valor.spi.plan.model.NotOperator;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionClause;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.plan.model.RelationSource;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

public class SemanticAnalyzer extends AstVisitorBase<LogicalPlanNode> {
  public static final String EQUAL = "equal";
  public static final String GREATERTHAN = "greaterthan";
  public static final String GREATERTHANOREQUAL = "greaterthanorequal";
  public static final String LESSTHAN = "lessthan";
  public static final String LESSTHANOREQUAL = "lessthanorequal";
  public static final String LIKE = "like";
  public static final String NOTEQUAL = "notequal";
  public static final String UNLIKE = "unlike";
  public static final String AND = "and";
  public static final String OR = "or";
  public static final String ISNULL = "isnull";
  public static final String ISNOTNULL = "isnotnull";
  public static final String NOT = "not";

  public static final List<String> BINARY_COMPARE_OPERATOR = Arrays.asList(EQUAL, GREATERTHAN,
      GREATERTHANOREQUAL, LESSTHAN, LESSTHANOREQUAL, LIKE, NOTEQUAL, UNLIKE);
  public static final List<String> LOGICAL_OPERATOR = Arrays.asList(AND, OR, NOT);
  public static final List<String> UNARY_OPERATOR = Arrays.asList(ISNULL, ISNOTNULL);

  private final SchemaRepository repository;

  protected Stack<Context> scope = new Stack<>();

  public SemanticAnalyzer(SchemaRepository repository) {
    this.repository = repository;
  }

  @Override
  protected boolean visitQueryNode(ScanNode node) {
    FromClauseNode from = node.getFrom();
    Context context = new Context();
    if (from instanceof RelationSourceNode) {
      String relationId = ((RelationSourceNode) from).getRelationId();
      try {
        Relation relation = repository.findRelation(relationId);
        for (String attr : relation.getAttributeNames()) {
          context.setType(attr, relation.getAttribute(attr).type());
        }
      } catch (ValorException e) {
        throw new IllegalStateException(e);
      }
    } else {
      throw new UnsupportedOperationException("unsupported source " + from);
    }
    scope.push(context);
    return true;
  }

  @Override
  protected void leaveQueryNode(ScanNode node) {
    scope.pop();
    List<LogicalPlanNode> exps = childResult.peek();
    ProjectionClause projection = (ProjectionClause) exps.get(0);
    RelationSource from = (RelationSource) exps.get(1);
    PredicativeExpression where = (PredicativeExpression) exps.get(2);
    this.result = new RelationScan(projection, from, where);
  }

  @Override
  protected void leaveProjectionClauseNode(ProjectionClauseNode node) {
    List<Expression> items = new ArrayList<>();
    Context context = scope.peek();
    for (LogicalPlanNode exp : childResult.peek()) {
      if (exp instanceof AttributeNameExpression) {
        items.add((Expression) exp);
      } else if (exp instanceof AllItem) {
        for (Map.Entry<String, AttributeType> entry : context.getEntries()) {
          items.add(new AttributeNameExpression(entry.getKey(), entry.getValue()));
        }
      }
    }
    this.result = new ProjectionClause(items);
  }

  @Override
  protected void leaveRelationSourceNode(RelationSourceNode node) {
    try {
      Relation relation = repository.findRelation(node.getRelationId());
      this.result = new RelationSource(relation);
    } catch (ValorException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  protected void leaveWhereClauseNode(WhereClauseNode node) {
    this.result = childResult.peek().get(0);
  }

  @Override
  protected void leaveAttributeNode(AttributeNode node) {
    Context context = scope.peek();
    String attrName = node.getAttribute();
    AttributeType type = context.getType(attrName);
    this.result = new AttributeNameExpression(attrName, type);
  }

  @Override
  protected void leaveConstantNode(ConstantNode node) {
    this.result = new ConstantExpression(node.getValue());
  }

  @Override
  protected void leaveFunctionNode(FunctionNode node) {
    String name = node.getName();
    List<LogicalPlanNode> arguments = childResult.peek();
    if (BINARY_COMPARE_OPERATOR.contains(name)) {
      LogicalPlanNode arg1 = arguments.get(0);
      LogicalPlanNode arg2 = arguments.get(1);
      AttributeNameExpression attr;
      ConstantExpression value;
      if (arg1 instanceof AttributeNameExpression) {
        attr = (AttributeNameExpression) arg1;
        value = (ConstantExpression) arg2;
      } else {
        attr = (AttributeNameExpression) arg2;
        value = (ConstantExpression) arg1;
      }
      this.result = buildBinaryPrimitivePredicate(name, attr, value);
    } else if (UNARY_OPERATOR.contains(name)) {
      this.result = buildUnaryPrimitivePredicate(name, (AttributeNameExpression) arguments.get(0));
    } else if (LOGICAL_OPERATOR.contains(name)) {
      this.result = buildLogicalOperator(name, arguments);
    } else {
      throw new IllegalArgumentException("unsupported function " + name);
    }
  }

  private PredicativeExpression buildBinaryPrimitivePredicate(
      String name, AttributeNameExpression attr, ConstantExpression value) {
    if (name.equals(EQUAL)) {
      return new EqualOperator(attr, value);
    } else if (name.equals(GREATERTHAN)) {
      return new GreaterthanOperator(attr, value);
    } else if (name.equals(GREATERTHANOREQUAL)) {
      return new GreaterthanorequalOperator(attr, value);
    } else if (name.equals(LESSTHAN)) {
      return new LessthanOperator(attr, value);
    } else if (name.equals(LESSTHANOREQUAL)) {
      return new LessthanorequalOperator(attr, value);
    } else if (name.equals(LIKE)) {
      return new RegexpOperator(attr, value);
    } else if (name.equals(UNLIKE)) {
      return new RegexpNotMatchOperator(attr, value);
    } else if (name.equals(NOTEQUAL)) {
      return new NotEqualOperator(attr, value);
    }
    throw new IllegalArgumentException("unsupported binary operator " + name);
  }

  private PredicativeExpression buildUnaryPrimitivePredicate(String name,
                                                             AttributeNameExpression attr) {
    if (ISNULL.equals(name)) {
      return new IsNullOperator(attr);
    } else if (ISNOTNULL.equals(name)) {
      return new IsNotNullOperator(attr);
    }
    throw new IllegalArgumentException("unsupported unary operator " + name);
  }

  private PredicativeExpression buildLogicalOperator(String name,
                                                             List<LogicalPlanNode> arguments) {
    if (AND.equals(name)) {
      AndOperator op = new AndOperator();
      arguments.forEach(a -> op.addOperand((PredicativeExpression) a));
      return op;
    } else if (OR.equals(name)) {
      OrOperator op = new OrOperator();
      arguments.forEach(a -> op.addOperand((PredicativeExpression) a));
      return op;
    } else if (NOT.equals(name)) {
      NotOperator op = new NotOperator((PredicativeExpression) arguments.get(0));
      return op;
    }
    throw new IllegalArgumentException("unsupported logical operator " + name);
  }

  /**
   * temporary marker expression
   */
  private static class AllItem implements Expression<Void> {

    @Override
    public void accept(LogicalPlanVisitor visitor) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Void apply(Tuple tuple) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AttributeType getType() {
      throw new UnsupportedOperationException();
    }
  }

  protected static class Context {
    private final LinkedHashMap<String, AttributeType> types = new LinkedHashMap<>();

    protected Iterable<Map.Entry<String, AttributeType>> getEntries() {
      return types.entrySet();
    }

    protected void setType(String name, AttributeType type) {
      types.put(name, type);
    }

    protected AttributeType getType(String name) {
      return types.get(name);
    }

  }
}
