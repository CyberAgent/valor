package jp.co.cyberagent.valor.ql.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.function.Function;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.ql.grammer.gen.ValorBaseListener;
import jp.co.cyberagent.valor.ql.grammer.gen.ValorParser;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.IsNotNullOperator;
import jp.co.cyberagent.valor.sdk.plan.function.IsNullOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.NotEqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.RegexpOperator;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfArrayValue;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfMapValue;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.DeleteStatement;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.InsertStatement;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;
import jp.co.cyberagent.valor.spi.plan.model.NotOperator;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.ParameterizedStatement;
import jp.co.cyberagent.valor.spi.plan.model.PlaceHolderExpression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionClause;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.plan.model.RelationSource;
import jp.co.cyberagent.valor.spi.plan.model.Udf;
import jp.co.cyberagent.valor.spi.plan.model.UdfExpression;
import jp.co.cyberagent.valor.spi.plan.model.UdpExpression;
import jp.co.cyberagent.valor.spi.plan.model.ValueClause;
import jp.co.cyberagent.valor.spi.plan.model.ValueItem;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.ArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.BooleanAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.schema.Schema;

public class BuildStatementListener extends ValorBaseListener {

  private LogicalPlanNode statement;

  private final ValorConnection conn;

  private Stack<List> subExpressions = new Stack<>();

  private Relation scope;

  private List<PlaceHolderExpression> placeholders = new ArrayList<>();

  public BuildStatementListener(ValorConnection conn) {
    this.conn = conn;
  }

  public LogicalPlanNode buildStatement() {
    return placeholders.isEmpty() ? statement : new ParameterizedStatement(statement, placeholders);
  }

  @Override
  public void enterDeleteStatementExpression(ValorParser.DeleteStatementExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
    String relationId = ctx.fromClause().relationSource().relationId.getText();
    try {
      scope = conn.findRelation(relationId);
    } catch (ValorException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void exitDeleteStatementExpression(ValorParser.DeleteStatementExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    RelationSource source = (RelationSource) children.get(0);
    PredicativeExpression condition = (PredicativeExpression) children.get(1);
    int limit = -1;
    if (ctx.limitClause() != null) {
      limit = Integer.parseInt(ctx.limitClause().num.getText());
    }

    DeleteStatement stmt = new DeleteStatement();
    stmt.setRelation(source);
    stmt.setCondition(condition);
    stmt.setLimit(limit);
    this.statement = stmt;
  }

  @Override
  public void enterInsertStatementExpression(ValorParser.InsertStatementExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitInsertStatementExpression(ValorParser.InsertStatementExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    RelationSource source = (RelationSource) children.get(0);
    ValueClause valueClause = (ValueClause) children.get(1);
    InsertStatement stmt = new InsertStatement();
    stmt.setRelation(source);
    valueClause.getItems().forEach(stmt::addValue);
    this.statement = stmt;
  }

  @Override
  public void enterValuesClause(ValorParser.ValuesClauseContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitValuesClause(ValorParser.ValuesClauseContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    ValueClause valueClause = new ValueClause();
    for (LogicalPlanNode item : children) {
      valueClause.addItem((ValueItem) item);
    }
    this.subExpressions.peek().add(valueClause);
  }

  @Override
  public void enterValueItem(ValorParser.ValueItemContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitValueItem(ValorParser.ValueItemContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    ValueItem item = new ValueItem();
    for (LogicalPlanNode v : children) {
      item.addValue((ConstantExpression) v);
    }
    this.subExpressions.peek().add(item);
  }

  @Override
  public void enterSelectStatementExpression(ValorParser.SelectStatementExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
    String relationId = ctx.fromClause().relationSource().relationId.getText();
    try {
      scope = conn.findRelation(relationId);
    } catch (ValorException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public void exitSelectStatementExpression(ValorParser.SelectStatementExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    RelationScan.Builder builder = new RelationScan.Builder();
    builder.setItems((ProjectionClause) children.get(0));
    builder.setRelationSource((RelationSource) children.get(1));
    if (children.size() > 2) {
      PredicativeExpression where = (PredicativeExpression) children.get(2);
      builder.setCondition(where);
    }
    if (ctx.limitClause() != null) {
      builder.setLimit(Integer.parseInt(ctx.limitClause().num.getText()));
    }

    this.statement = builder.build();
  }

  @Override
  public void enterSelectItems(ValorParser.SelectItemsContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitSelectItems(ValorParser.SelectItemsContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    List<Expression> items = children.stream()
        .map(Expression.class::cast).collect(Collectors.toList());
    this.subExpressions.peek().add(new ProjectionClause(items));
  }

  @Override
  public void enterSelectItem(ValorParser.SelectItemContext ctx) {
    if (ctx.STAR() != null) {
      scope.getAttributes().forEach(a -> {
        this.subExpressions.peek().add(new AttributeNameExpression(a.name(), a.type()));
      });
    }
  }


  @Override
  public void exitRelationSource(ValorParser.RelationSourceContext ctx) {
    String relationId = ctx.relationId.getText();
    RelationSource source;
    try {
      Relation relation = conn.findRelation(relationId);
      if (ctx.schemaId != null) {
        Schema schema = conn.findSchema(relationId, ctx.schemaId.getText());
        source = new RelationSource(relation, schema);
      } else {
        source = new RelationSource(relation);
      }
    } catch (ValorException e) {
      throw new IllegalArgumentException(e);
    }
    this.subExpressions.peek().add(source);
  }

  @Override
  public void enterWhereClause(ValorParser.WhereClauseContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitWhereClause(ValorParser.WhereClauseContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    if (children.size() != 1) {
      throw new IllegalArgumentException("only one expression is expected in where");
    }
    this.subExpressions.peek().add(children.get(0));
  }

  @Override
  public void enterOrExpression(ValorParser.OrExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitOrExpression(ValorParser.OrExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    if (children.isEmpty()) {
      throw new IllegalArgumentException("no expression found");
    } else if (children.size() == 1) {
      this.subExpressions.peek().add(children.get(0));
    } else {
      List<PredicativeExpression> predicates = children.stream()
          .map(PredicativeExpression.class::cast).collect(Collectors.toList());
      this.subExpressions.peek().add(OrOperator.join(predicates));
    }
  }

  @Override
  public void enterAndExpression(ValorParser.AndExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitAndExpression(ValorParser.AndExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    if (children.isEmpty()) {
      throw new IllegalArgumentException("no expression found");
    } else if (children.size() == 1) {
      this.subExpressions.peek().add(children.get(0));
    } else {
      List<PredicativeExpression> predicates = children.stream()
          .map(PredicativeExpression.class::cast).collect(Collectors.toList());
      this.subExpressions.peek().add(AndOperator.join(predicates));
    }
  }

  @Override
  public void enterNotExpression(ValorParser.NotExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitNotExpression(ValorParser.NotExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    if (children.size() != 1) {
      throw new IllegalArgumentException("only one expression is expected for NotExpression, but "
          + children.size() + "(" + ctx.getText() + ")");
    }
    LogicalPlanNode exp = children.get(0);
    this.subExpressions.peek()
        .add(ctx.notOperator() == null ? exp : new NotOperator((PredicativeExpression) exp));
  }

  @Override
  public void enterBinaryPredicateExpression(ValorParser.BinaryPredicateExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitBinaryPredicateExpression(ValorParser.BinaryPredicateExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    if (children.size() != 2) {
      throw new IllegalArgumentException(
          "two expressions are expected for binary predicate, but " + children.size());
    }
    Expression left = (Expression) children.get(0);
    Expression right = (Expression) children.get(1);
    Expression predicate = null;
    ValorParser.CompareOperatorContext op = ctx.compareOperator();
    if (op.EQUAL() != null) {
      predicate = new EqualOperator(left, right);
    } else if (op.GREATERTHAN() != null) {
      predicate = new GreaterthanOperator(left, right);
    } else if (op.GREATERTHANOREQUAL() != null) {
      predicate = new GreaterthanorequalOperator(left, right);
    } else if (op.LESSTHAN() != null) {
      predicate = new LessthanOperator(left, right);
    } else if (op.LESSTHANOREQUAL() != null) {
      predicate = new LessthanorequalOperator(left, right);
    } else if (op.NOTEQUAL() != null) {
      predicate = new NotEqualOperator(left, right);
    } else if (op.LIKE() != null || op.REGEXP() != null) {
      if (right instanceof ConstantExpression) {
        Object pattern = ((ConstantExpression) right).getValue();
        if (op.LIKE() != null) {
          if (pattern instanceof String) {
            pattern = ((String)pattern).replaceAll("%", ".*").replaceAll("_", ".");
          } else if (pattern instanceof Map) {
            pattern = ((Map)pattern).entrySet().stream().collect(Collectors.toMap(
                Function.identity(),
                v -> ((String)v).replaceAll("%", ".*").replaceAll("_", ".")
            ));
          }
        }
        right = new ConstantExpression(pattern);
        predicate = new RegexpOperator(left, right);
      } else {
        throw new IllegalArgumentException("like operator expects a constant string, but " + right);
      }
    } else {
      throw new IllegalArgumentException("unsupported operator " + op.getText());
    }
    this.subExpressions.peek().add(predicate);
  }

  @Override
  public void enterPlusOperator(ValorParser.PlusOperatorContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitPlusOperator(ValorParser.PlusOperatorContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    if (children.size() != 1) {
      throw new IllegalArgumentException("plus operator is not supported currently");
    }
    this.subExpressions.peek().add(children.get(0));;
  }

  @Override
  public void enterMultiplyExpression(ValorParser.MultiplyExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitMultiplyExpression(ValorParser.MultiplyExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    if (children.size() != 1) {
      throw new IllegalArgumentException("multipley operator is not supported currently");
    }
    this.subExpressions.peek().add(children.get(0));;
  }

  @Override
  public void enterIsNullExpression(ValorParser.IsNullExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitIsNullExpression(ValorParser.IsNullExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    if (children.size() != 1) {
      throw new IllegalArgumentException("only one expression is expected for NotExpression, but "
          + children.size() + "(" + ctx.getText() + ")");
    }
    Expression exp = (Expression) children.get(0);
    exp = ctx.KW_NOT() == null ? new IsNullOperator(exp) : new IsNotNullOperator(exp);
    this.subExpressions.peek().add(exp);
  }

  @Override
  public void enterAttributeName(ValorParser.AttributeNameContext ctx) {
    Relation.Attribute attr = scope.getAttribute(ctx.getText());
    this.subExpressions.peek().add(new AttributeNameExpression(attr.name(), attr.type()));
  }

  @Override
  public void enterCollectionElementExpression(ValorParser.CollectionElementExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitCollectionElementExpression(ValorParser.CollectionElementExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    Expression collection = (Expression) children.get(0);
    if (children.size() == 1) {
      this.subExpressions.peek().add(collection);
    } else {
      Expression index = (Expression) children.get(1);
      if (collection.getType() instanceof MapAttributeType) {
        this.subExpressions.peek()
            .add(new UdfExpression(new UdfMapValue(), Arrays.asList(collection, index)));
      } else if (collection.getType() instanceof ArrayAttributeType) {
        this.subExpressions.peek()
            .add(new UdfExpression(new UdfArrayValue(), Arrays.asList(collection, index)));
      } else {
        throw new IllegalArgumentException("unexpected collection " + collection);
      }
    }
  }

  @Override
  public void enterFunctionExpression(ValorParser.FunctionExpressionContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitFunctionExpression(ValorParser.FunctionExpressionContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    String funcName = ctx.functionName.getText();
    List<Expression> args = children.stream()
        .map(Expression.class::cast).collect(Collectors.toList());
    Udf udf = conn.getContext().createUdf(funcName);
    Expression evaluator;
    if (udf.getReturnType() instanceof BooleanAttributeType) {
      evaluator = new UdpExpression(udf, args);
    } else {
      evaluator = new UdfExpression(udf, args);
    }
    this.subExpressions.peek().add(evaluator);
  }

  @Override
  public void enterConstant(ValorParser.ConstantContext ctx) {
    this.subExpressions.push(new ArrayList());
  }

  @Override
  public void exitConstant(ValorParser.ConstantContext ctx) {
    List<LogicalPlanNode> children = this.subExpressions.pop();
    Expression constant = null;
    if (ctx.IntegerLiteral() != null) {
      constant = new ConstantExpression(Integer.parseInt(ctx.getText()));
    } else if (ctx.LongLiteral() != null) {
      String v = ctx.getText();
      constant = new ConstantExpression(Long.parseLong(v.substring(0, v.length() - 1)));
    } else if (ctx.DoubleLiteral() != null) {
      constant = new ConstantExpression(Double.parseDouble(ctx.getText()));
    } else if (ctx.FloatLiteral() != null) {
      constant = new ConstantExpression(Float.parseFloat(ctx.getText()));
    } else if (ctx.StringLiteral() != null) {
      String v = ctx.getText();
      constant = new ConstantExpression(v.substring(1, v.length() - 1));
    } else if (ctx.arrayLiteral() != null) {
      List l = children.stream()
          .map(e -> ((ConstantExpression)e).getValue()).collect(Collectors.toList());
      constant = new ConstantExpression(l);
    } else if (ctx.mapLiteral() != null) {
      Map m = new HashMap<>();
      for (int i = 0; i < children.size(); i += 2) {
        ConstantExpression k = (ConstantExpression) children.get(i);
        ConstantExpression v = (ConstantExpression) children.get(i + 1);
        m.put(k.getValue(), v.getValue());
      }
      constant = new ConstantExpression(m);
    } else if (ctx.QUESTION() != null) {
      constant = new PlaceHolderExpression();
      this.placeholders.add((PlaceHolderExpression) constant);
    } else {
      throw new IllegalArgumentException("unsupported constant " + ctx.getText());
    }
    this.subExpressions.peek().add(constant);
  }
}
