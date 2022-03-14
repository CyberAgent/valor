package jp.co.cyberagent.valor.hive;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.IsNullOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.NotEqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.RegexpOperator;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.NotOperator;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIndex;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRegExp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExprNodeToExpressionVisitor extends ExprNodeVisitor<PredicativeExpression> {

  static final Logger LOG = LoggerFactory.getLogger(ExprNodeToExpressionVisitor.class);

  private Relation relation;

  public ExprNodeToExpressionVisitor(Relation relation, Configuration conf) {
    this.relation = relation;
  }

  @Override
  protected boolean doVisitGenericFuncDesc(ExprNodeGenericFuncDesc node) {
    return FunctionRegistry.isOpAnd(node) || FunctionRegistry.isOpOr(node) || FunctionRegistry
        .isOpNot(node);
  }

  @Override
  protected void doLeaveGenericFuncDesc(ExprNodeGenericFuncDesc node) {
    if (FunctionRegistry.isOpAnd(node)) {
      List<PredicativeExpression> childResult = this.childResults.peek();
      this.result = AndOperator.join(childResult);
    } else if (FunctionRegistry.isOpOr(node)) {
      List<PredicativeExpression> childResult = this.childResults.peek();
      this.result = OrOperator.join(childResult);
    } else if (FunctionRegistry.isOpNot(node)) {
      List<PredicativeExpression> childResult = this.childResults.peek();
      Preconditions.checkState(childResult.size() == 1);
      this.result = new NotOperator(childResult.get(0));
    } else {
      List<ExprNodeDesc> args = node.getChildren();
      PredicativeExpression pred = toPredicativeExpression(node, args);
      this.result = pred;
    }
  }

  private PredicativeExpression toPredicativeExpression(ExprNodeGenericFuncDesc node,
                                                        List<ExprNodeDesc> args) {
    if (FunctionRegistry.isIn(node)) {
      String attrName = extractAttributeName(args.get(0));
      if (attrName == null) {
        LOG.warn(args.get(0) + " is not an attribute name expression, unable to push down");
        return ConstantExpression.TRUE;
      }
      AttributeType attrType = relation.getAttribute(attrName).type();
      List<PredicativeExpression> disjunction = new ArrayList<>(args.size() - 1);
      for (int i = 1; i < args.size(); i++) {
        if (!(args.get(i) instanceof ExprNodeConstantDesc)) {
          LOG.warn(args.get(i) + " is not a constant, unable to push down");
          return ConstantExpression.TRUE;
        }
        ExprNodeConstantDesc val = (ExprNodeConstantDesc) args.get(i);
        disjunction.add(new EqualOperator(attrName, attrType, val.getValue()));
      }
      return OrOperator.join(disjunction);
    } else if (args.size() == 1) {
      if (node.getGenericUDF() instanceof GenericUDFOPNull) {
        String attrName = extractAttributeName(args.get(0));
        Relation.Attribute attr = relation.getAttribute(attrName);
        return new IsNullOperator(new AttributeNameExpression(attrName, attr.type()));
      }
    } else if (args.size() == 2) {
      // binary primitive operators
      PredicativeExpression pred = toBinaryPredicate(node, args);
      if (pred != null) {
        return pred;
      }
    }
    LOG.warn(node.getExprString() + "[" + node.getGenericUDF().getClass() + " with " + args
        .size() + " arguments ] cannot be pushed down");
    return ConstantExpression.TRUE;
  }

  private PredicativeExpression toBinaryPredicate(ExprNodeGenericFuncDesc node,
                                                  List<ExprNodeDesc> args) {
    OpType opType = genericUdf2OpType(node.getGenericUDF());
    String attr = extractAttributeName(args.get(0));
    AttributeType type = relation.getAttributeType(attr);
    if (attr == null) {
      LOG.warn(args.get(0) + " is not an attribute name expression, unable to push down");
      return null;
    }
    if (!(args.get(1) instanceof ExprNodeConstantDesc)) {
      LOG.warn(args.get(1) + " is not a constant, unable to push down");
      return null;
    }
    ExprNodeConstantDesc right = (ExprNodeConstantDesc) args.get(1);
    return opType.build(new AttributeNameExpression(attr, type), right.getValue());
  }

  private String extractAttributeName(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeColumnDesc) {
      return ((ExprNodeColumnDesc) expr).getColumn();
    } else if (expr instanceof ExprNodeGenericFuncDesc) {
      GenericUDF func = ((ExprNodeGenericFuncDesc) expr).getGenericUDF();
      // should be a indexed map value expression
      if (!(func instanceof GenericUDFIndex)) {
        return null;
      }
      ExprNodeDesc colExpr = expr.getChildren().get(0);
      ExprNodeDesc indexExpr = expr.getChildren().get(1);
      if (!(colExpr instanceof ExprNodeColumnDesc && indexExpr instanceof ExprNodeConstantDesc)) {
        return null;
      }
      Object index = ((ExprNodeConstantDesc) indexExpr).getValue();
      if (index instanceof String) {
        return (String) index;
      }
    }
    return null;
  }

  private OpType genericUdf2OpType(GenericUDF genericUdf) {
    Class<? extends GenericUDF> udfClass = genericUdf.getClass();
    if (GenericUDFOPEqual.class.equals(udfClass)) {
      return OpType.EQUQL;
    } else if (GenericUDFOPNotEqual.class.equals(udfClass)) {
      return OpType.NOTEQUAL;
    } else if (GenericUDFOPLessThan.class.equals(udfClass)) {
      return OpType.LESSTHAN;
    } else if (GenericUDFOPEqualOrLessThan.class.equals(udfClass)) {
      return OpType.LESSTHANOREQUAL;
    } else if (GenericUDFOPGreaterThan.class.equals(udfClass)) {
      return OpType.GREATERTHAN;
    } else if (GenericUDFOPEqualOrGreaterThan.class.equals(udfClass)) {
      return OpType.GREATERTHANOREQUAL;
    } else if (GenericUDFRegExp.class.equals(udfClass)) {
      return OpType.REGEXP;
    } else if (GenericUDFBridge.class.equals(udfClass)) {
      Class<? extends UDF> wrappedUdfClass = ((GenericUDFBridge) genericUdf).getUdfClass();
      if (UDFLike.class.equals(wrappedUdfClass)) {
        return OpType.LIKE;
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  private enum OpType {
    EQUQL {
      @Override
      protected PredicativeExpression build(AttributeNameExpression attr, Object val) {
        return new EqualOperator(attr, new ConstantExpression(val));
      }
    }, NOTEQUAL {
      @Override
      protected PredicativeExpression build(AttributeNameExpression attr, Object val) {
        return new NotEqualOperator(attr, new ConstantExpression(val));
      }
    }, LIKE {
      @Override
      protected PredicativeExpression build(AttributeNameExpression attr, Object val) {
        if (val == null) {
          return ConstantExpression.FALSE;
        }
        String v = val.toString().replace("%", ".*").replaceAll("_", ".");
        return new RegexpOperator(attr,new ConstantExpression(v));
      }
    }, REGEXP {
      @Override
      protected PredicativeExpression build(AttributeNameExpression attr, Object val) {
        if (val == null) {
          return ConstantExpression.FALSE;
        }
        return new RegexpOperator(attr,new ConstantExpression(val));
      }
    }, LESSTHAN {
      @Override
      protected PredicativeExpression build(AttributeNameExpression attr, Object val) {
        return new LessthanOperator(attr, new ConstantExpression(val));
      }
    }, LESSTHANOREQUAL {
      @Override
      protected PredicativeExpression build(AttributeNameExpression attr, Object val) {
        return new LessthanorequalOperator(attr, new ConstantExpression(val));
      }
    }, GREATERTHAN {
      @Override
      protected PredicativeExpression build(AttributeNameExpression attr, Object val) {
        return new GreaterthanOperator(attr, new ConstantExpression(val));
      }
    }, GREATERTHANOREQUAL {
      @Override
      protected PredicativeExpression build(AttributeNameExpression attr, Object val) {
        return new GreaterthanorequalOperator(attr, new ConstantExpression(val));
      }
    };

    protected abstract PredicativeExpression build(AttributeNameExpression attr, Object val);
  }
}
