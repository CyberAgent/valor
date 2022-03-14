package jp.co.cyberagent.valor.ql.parse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.IsNotNullOperator;
import jp.co.cyberagent.valor.sdk.plan.function.IsNullOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.NotEqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.RegexpOperator;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfArrayContains;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfMapKeys;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfMapValue;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;
import jp.co.cyberagent.valor.spi.plan.model.NotOperator;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.plan.model.UdfExpression;
import jp.co.cyberagent.valor.spi.plan.model.UdpExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.ArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.BooleanAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.DoubleAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.FloatAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestParseSelectStatement {

  static final ArrayAttributeType ARRAY_TYPE
      = ArrayAttributeType.create(StringAttributeType.INSTANCE);
  static final MapAttributeType MAP_TYPE
      = MapAttributeType.create(StringAttributeType.INSTANCE, StringAttributeType.INSTANCE);


  static final Relation relation = ImmutableRelation.builder().relationId("relId")
      .addAttribute("k", true, StringAttributeType.INSTANCE)
      .addAttribute("i", false, IntegerAttributeType.INSTANCE)
      .addAttribute("l", false, LongAttributeType.INSTANCE)
      .addAttribute("d", false, DoubleAttributeType.INSTANCE)
      .addAttribute("f", false, FloatAttributeType.INSTANCE)
      .addAttribute("b", false, BooleanAttributeType.INSTANCE)
      .addAttribute("a", false, ARRAY_TYPE)
      .addAttribute("m", false, MAP_TYPE)
      .build();

  static final AttributeNameExpression KEY_ATTR_EXP
      = new AttributeNameExpression("k", StringAttributeType.INSTANCE);
  static final AttributeNameExpression INT_ATTR_EXP
      = new AttributeNameExpression("i", IntegerAttributeType.INSTANCE);
  static final AttributeNameExpression LONG_ATTR_EXP
      = new AttributeNameExpression("l", LongAttributeType.INSTANCE);
  static final AttributeNameExpression DOUBLE_ATTR_EXP
      = new AttributeNameExpression("d", DoubleAttributeType.INSTANCE);
  static final AttributeNameExpression FLOAT_ATTR_EXP
      = new AttributeNameExpression("f", FloatAttributeType.INSTANCE);
  static final AttributeNameExpression BOOLEAN_ATTR_EXP
      = new AttributeNameExpression("b", BooleanAttributeType.INSTANCE);
  static final AttributeNameExpression ARRAY_ATTR_EXP
      = new AttributeNameExpression("a", ARRAY_TYPE);
  static final AttributeNameExpression MAP_ATTR_EXP
      = new AttributeNameExpression("m", MAP_TYPE);

  static final AttributeNameExpression[] ATTR_EXPS = {
      KEY_ATTR_EXP, INT_ATTR_EXP, LONG_ATTR_EXP, DOUBLE_ATTR_EXP, FLOAT_ATTR_EXP, BOOLEAN_ATTR_EXP,
      ARRAY_ATTR_EXP, MAP_ATTR_EXP};

  static ValorConnection conn;

  @BeforeAll
  public static void init() throws ValorException {
    conn = ValorConnectionFactory.create(StandardContextFactory.create());
    conn.createRelation(relation, true);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    conn.close();
  }

  public static Fixture[] fixtures = {
      new Fixture("SELECT * FROM relId WHERE i = 1 AND l = 2l AND d = 0.1 AND f = 0.2f",
          new RelationScan.Builder()
              .setItems(ATTR_EXPS)
              .setRelationSource(null, relation)
              .setCondition(AndOperator.join(
                  new EqualOperator(INT_ATTR_EXP, new ConstantExpression(1)),
                  new EqualOperator(LONG_ATTR_EXP, new ConstantExpression(2l)),
                  new EqualOperator(DOUBLE_ATTR_EXP, new ConstantExpression(0.1)),
                  new EqualOperator(FLOAT_ATTR_EXP, new ConstantExpression(0.2F))
              ))
              .build()),
      new Fixture("SELECT i FROM relId WHERE k = '100'",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(new EqualOperator(KEY_ATTR_EXP, new ConstantExpression("100")))
              .build()),
      new Fixture("SELECT i FROM relId WHERE k >= 'a' AND k <= 'z'",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(AndOperator.join(
                  new GreaterthanorequalOperator(KEY_ATTR_EXP, new ConstantExpression("a")),
                  new LessthanorequalOperator(KEY_ATTR_EXP, new ConstantExpression("z"))))
              .build()),
      new Fixture("SELECT i FROM relId WHERE k > 'a' OR k < 'z' AND k != 'n'",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(OrOperator.join(
                  new GreaterthanOperator(KEY_ATTR_EXP, new ConstantExpression("a")),
                  AndOperator.join(
                      new LessthanOperator(KEY_ATTR_EXP, new ConstantExpression("z")),
                      new NotEqualOperator(KEY_ATTR_EXP, new ConstantExpression("n"))
                  )))
              .build()),
      new Fixture("SELECT k, i FROM relId WHERE k IS NOT NULL AND i IS NULL AND NOT k IS NULL",
          new RelationScan.Builder()
              .setItems(KEY_ATTR_EXP, INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(AndOperator.join(
                  new IsNotNullOperator(KEY_ATTR_EXP),
                  new IsNullOperator(INT_ATTR_EXP),
                  new NotOperator(new IsNullOperator(KEY_ATTR_EXP))))
              .build()),
      new Fixture("SELECT i FROM relId WHERE array_contains(a, 'x')",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(new UdpExpression(new UdfArrayContains(),
                  Arrays.asList(ARRAY_ATTR_EXP, new ConstantExpression<>("x"))))
              .build()),
      new Fixture("SELECT i FROM relId WHERE a = array('1', '2')",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(new EqualOperator(
                  ARRAY_ATTR_EXP, new ConstantExpression(Arrays.asList("1", "2"))))
              .build()),
      new Fixture("SELECT i FROM relId WHERE m = map('k','v')",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(new EqualOperator(
                  MAP_ATTR_EXP, new ConstantExpression(Collections.singletonMap("k", "v"))))
              .build()),
      new Fixture("SELECT i FROM relId WHERE map_keys(m) = array('k1','k2')",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(new EqualOperator(
                  new UdfExpression(new UdfMapKeys(), Arrays.asList(MAP_ATTR_EXP)),
                  new ConstantExpression(Arrays.asList("k1", "k2"))))
              .build()),
      new Fixture("SELECT i FROM relId WHERE m['k1'] = 'v1'",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(new EqualOperator(
                  new UdfExpression(
                      new UdfMapValue(),
                      Arrays.asList(MAP_ATTR_EXP, new ConstantExpression("k1"))),
                  new ConstantExpression("v1")))
              .build()),
      new Fixture("SELECT i FROM relId WHERE k like 'regexp%'",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(new RegexpOperator(KEY_ATTR_EXP, new ConstantExpression("regexp.*")))
              .build()),
      new Fixture("SELECT i FROM relId WHERE k REGEXP 'regexp.*'",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(new RegexpOperator(KEY_ATTR_EXP, new ConstantExpression("regexp.*")))
              .build()),
      new Fixture("SELECT i FROM relId WHERE m REGEXP map('key', 'val.*')",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(new RegexpOperator(
                  MAP_ATTR_EXP, new ConstantExpression(Collections.singletonMap("key", "val.*"))))
              .build()),
      new Fixture("SELECT i FROM relId",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .build()),
      new Fixture("SELECT i FROM relId WHERE k = '100' LIMIT 5",
          new RelationScan.Builder()
              .setItems(INT_ATTR_EXP)
              .setRelationSource(null, relation)
              .setCondition(new EqualOperator(KEY_ATTR_EXP, new ConstantExpression("100")))
              .setLimit(5)
              .build())
  };

  static Fixture[] getFixtures() {
    return fixtures;
  }


  @ParameterizedTest
  @MethodSource("getFixtures")
  public void test(Fixture fixture) throws Exception {
    Parser parser = new Parser(conn);
    LogicalPlanNode plan = parser.parseStatement(fixture.query);
    assertThat(fixture.toString(plan), plan, equalTo(fixture.expected));
  }

  static class Fixture {
    public final String query;
    public final RelationScan expected;

    public Fixture(String query, RelationScan expected) {
      this.query = query;
      this.expected = expected;
    }

    public String toString(LogicalPlanNode actual) {
      String nr = System.getProperty("line.separator");
      return nr + query + nr + actual.toString();
    }
  }
}
