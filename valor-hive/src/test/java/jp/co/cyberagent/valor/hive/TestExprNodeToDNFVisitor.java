package jp.co.cyberagent.valor.hive;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import java.lang.reflect.Constructor;
import java.util.Collections;
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
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.NotOperator;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.UDFLike;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFRegExp;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaBooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.JavaStringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestExprNodeToDNFVisitor {
  static JavaBooleanObjectInspector boolOI =
      PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  static JavaStringObjectInspector stringOI =
      PrimitiveObjectInspectorFactory.javaStringObjectInspector;

  static final String COL1 = "col1";
  static final String COL2 = "col2";
  static final String COL3 = "col3";

  static Relation relation = ImmutableRelation.builder().relationId("test")
      .addAttribute(COL1, true, StringAttributeType.INSTANCE)
      .addAttribute(COL2, true, StringAttributeType.INSTANCE)
      .addAttribute(COL3, false, StringAttributeType.INSTANCE)
      .build();


  static class PredicateFixture {
    public final Class<? extends BinaryPrimitivePredicate> valorOpCls;
    public final Object valorVal;
    public final GenericUDF hiveUdf;
    public final Object hiveVal;

    PredicateFixture(Class<? extends BinaryPrimitivePredicate> valorOpCls, Object valorVal,
                     GenericUDF hiveUdf, Object hiveVal) {
      this.valorOpCls = valorOpCls;
      this.valorVal = valorVal;
      this.hiveUdf = hiveUdf;
      this.hiveVal = hiveVal;
    }
  }

  public static PredicateFixture[] fixtures = {
      new PredicateFixture(EqualOperator.class, "val", new GenericUDFOPEqual(), "val"),
      new PredicateFixture(RegexpOperator.class, ".*val",
          new GenericUDFBridge("like", true, UDFLike.class.getName()), "%val"),
      new PredicateFixture(RegexpOperator.class, "%val", new GenericUDFRegExp(), "%val"),
      new PredicateFixture(NotEqualOperator.class, "val", new GenericUDFOPNotEqual(), "val"),
      new PredicateFixture(LessthanOperator.class, "val", new GenericUDFOPLessThan(), "val"),
      new PredicateFixture(LessthanorequalOperator.class, "val",
          new GenericUDFOPEqualOrLessThan(), "val"),
      new PredicateFixture(GreaterthanOperator.class, "val",
          new GenericUDFOPGreaterThan(), "val"),
      new PredicateFixture(GreaterthanorequalOperator.class, "val",
          new GenericUDFOPEqualOrGreaterThan(), "val"),
  };

  static PredicateFixture[] getFixtures() {
    return fixtures;
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testBinaryPredicate(PredicateFixture fixture) throws Exception {
    Constructor constructor
        = fixture.valorOpCls.getConstructor(Expression.class, Expression.class);
    Expression valorExp = (Expression) constructor.newInstance(
        new AttributeNameExpression(COL1, StringAttributeType.INSTANCE),
        new ConstantExpression(fixture.valorVal));

    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(String.class, COL1, null, false);
    ExprNodeConstantDesc valExpr = new ExprNodeConstantDesc(fixture.hiveVal);
    ExprNodeDesc cond = new ExprNodeGenericFuncDesc(
        boolOI, fixture.hiveUdf, Lists.newArrayList(colExpr, valExpr));

    ExprNodeToExpressionVisitor visitor
        = new ExprNodeToExpressionVisitor(relation, new Configuration(false));

    Expression actual = visitor.walk(cond);
    // TODO implements equals
    assertEquals(valorExp.toString(), actual.toString());

  }

  // Test for unsupported predicates be transformed a true constant expression.
  @Test
  public void testUnsupportedPredicate() throws Exception {
    GenericUDF unknownUdf = new GenericUDF() {
      @Override
      public ObjectInspector initialize(ObjectInspector[] objectInspectors) {
        return null;
      }

      @Override
      public Object evaluate(DeferredObject[] deferredObjects) {
        return null;
      }

      @Override
      public String getDisplayString(String[] strings) {
        return null;
      }
    };

    ExprNodeDesc cond = new ExprNodeGenericFuncDesc(boolOI, unknownUdf, Collections.emptyList());

    ExprNodeToExpressionVisitor visitor
        = new ExprNodeToExpressionVisitor(relation, new Configuration(false));

    Expression actual = visitor.walk(cond);
    // TODO implements equals
    assertEquals(ConstantExpression.TRUE, actual);
  }

  @Test
  public void testAndCondition() throws Exception {
    ExprNodeDesc cond1 = buildEqualCondition("col1", "val1");
    ExprNodeDesc cond2 = buildEqualCondition("col2", "val2");
    ExprNodeDesc cond3 = buildEqualCondition("col3", "val3");
    ExprNodeDesc cond = new ExprNodeGenericFuncDesc(boolOI, new GenericUDFOPAnd(),
        Lists.newArrayList(cond1, cond2, cond3));
    ExprNodeToExpressionVisitor visitor = new ExprNodeToExpressionVisitor(relation,
        new Configuration(false));
    AndOperator actual = (AndOperator) visitor.walk(cond);
    EqualOperator first = (EqualOperator) actual.getOperands().get(0);
    assertThat(first.getLeft(), hasProperty("name", equalTo("col1")));
    assertThat(first.getRight(), hasProperty("value", equalTo("val1")));
    EqualOperator second = (EqualOperator) actual.getOperands().get(1);
    assertThat(second.getLeft(), hasProperty("name", equalTo("col2")));
    assertThat(second.getRight(), hasProperty("value", equalTo("val2")));
    EqualOperator third = (EqualOperator) actual.getOperands().get(2);
    assertThat(third.getLeft(), hasProperty("name", equalTo("col3")));
    assertThat(third.getRight(), hasProperty("value", equalTo("val3")));
  }

  @Test
  public void testOrCondition() throws Exception {
    ExprNodeDesc cond1 = buildEqualCondition("col1", "val1");
    ExprNodeDesc cond2 = buildEqualCondition("col2", "val2");
    ExprNodeDesc cond3 = buildEqualCondition("col3", "val3");
    ExprNodeDesc cond = new ExprNodeGenericFuncDesc(boolOI, new GenericUDFOPOr(),
        Lists.newArrayList(cond1, cond2, cond3));
    ExprNodeToExpressionVisitor visitor = new ExprNodeToExpressionVisitor(relation,
        new Configuration(false));
    OrOperator actual = (OrOperator) visitor.walk(cond);
    EqualOperator first = (EqualOperator) actual.getOperands().get(0);
    assertThat(first.getLeft(), hasProperty("name", equalTo("col1")));
    assertThat(first.getRight(), hasProperty("value", equalTo("val1")));
    EqualOperator second = (EqualOperator) actual.getOperands().get(1);
    assertThat(second.getLeft(), hasProperty("name", equalTo("col2")));
    assertThat(second.getRight(), hasProperty("value", equalTo("val2")));
    EqualOperator third = (EqualOperator) actual.getOperands().get(2);
    assertThat(third.getLeft(), hasProperty("name", equalTo("col3")));
    assertThat(third.getRight(), hasProperty("value", equalTo("val3")));
  }

  @Test
  public void testInCondition() throws Exception {
    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(String.class, COL1, null, false);
    ExprNodeConstantDesc val1Expr = new ExprNodeConstantDesc("val1");
    ExprNodeConstantDesc val2Expr = new ExprNodeConstantDesc("val2");
    ExprNodeConstantDesc val3Expr = new ExprNodeConstantDesc("val3");
    ExprNodeDesc cond = new ExprNodeGenericFuncDesc(boolOI, new GenericUDFIn(),
        Lists.newArrayList(colExpr, val1Expr, val2Expr, val3Expr));
    ExprNodeToExpressionVisitor visitor = new ExprNodeToExpressionVisitor(relation,
        new Configuration(false));
    OrOperator actual = (OrOperator) visitor.walk(cond);
    EqualOperator first = (EqualOperator) actual.getOperands().get(0);
    assertThat(first.getLeft(), hasProperty("name", equalTo("col1")));
    assertThat(first.getRight(), hasProperty("value", equalTo("val1")));
    EqualOperator second = (EqualOperator) actual.getOperands().get(1);
    assertThat(second.getLeft(), hasProperty("name", equalTo("col1")));
    assertThat(second.getRight(), hasProperty("value", equalTo("val2")));
    EqualOperator third = (EqualOperator) actual.getOperands().get(2);
    assertThat(third.getLeft(), hasProperty("name", equalTo("col1")));
    assertThat(third.getRight(), hasProperty("value", equalTo("val3")));
  }

  @Test
  public void testNotCondition() throws Exception {
    ExprNodeDesc cond1 = buildEqualCondition("col1", "val1");
    ExprNodeDesc cond = new ExprNodeGenericFuncDesc(boolOI, new GenericUDFOPNot(),
        Lists.newArrayList(cond1));
    ExprNodeToExpressionVisitor visitor = new ExprNodeToExpressionVisitor(relation,
        new Configuration(false));
    NotOperator actual = (NotOperator) visitor.walk(cond);
    EqualOperator operand = (EqualOperator) actual.getOperand();
    assertThat(operand.getLeft(), hasProperty("name", equalTo("col1")));
    assertThat(operand.getRight(), hasProperty("value", equalTo("val1")));
  }

  @Test
  public void testIsNullCondition() throws Exception {
    ExprNodeDesc cond = new ExprNodeGenericFuncDesc(boolOI, new GenericUDFOPNull(),
        Lists.newArrayList(new ExprNodeColumnDesc(String.class, "col1", null,
            false)));
    ExprNodeToExpressionVisitor visitor = new ExprNodeToExpressionVisitor(relation,
        new Configuration(false));
    IsNullOperator actual = (IsNullOperator) visitor.walk(cond);
    assertThat(actual.getOperand(), hasProperty("name", equalTo("col1")));
  }

  private ExprNodeDesc buildEqualCondition(String col, String val) {
    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(String.class, col, null, false);
    ExprNodeConstantDesc valExpr = new ExprNodeConstantDesc(val);
    return new ExprNodeGenericFuncDesc(boolOI, new GenericUDFOPEqual(),
        Lists.newArrayList(colExpr, valExpr));
  }
}
