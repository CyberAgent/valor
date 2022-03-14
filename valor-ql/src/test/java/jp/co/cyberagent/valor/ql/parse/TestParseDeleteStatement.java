package jp.co.cyberagent.valor.ql.parse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.DeleteStatement;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.RelationSource;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestParseDeleteStatement {

  static final Relation relation = ImmutableRelation.builder().relationId("relId")
      .addAttribute("k", true, StringAttributeType.INSTANCE)
      .addAttribute("v", false, IntegerAttributeType.INSTANCE)
      .build();

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

  static Fixture[] getFixtures() {
    return new Fixture[] {
        new Fixture("DELETE FROM relId WHERE k = 100", relation,
            new EqualOperator(
                new AttributeNameExpression("k", StringAttributeType.INSTANCE),
                new ConstantExpression(100)))
    };
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void test(Fixture fixture) {
    Parser parser = new Parser(conn);
    LogicalPlanNode plan = parser.parseStatement(fixture.statement);
    assertThat(fixture.statement, plan, equalTo(fixture.buildStatement()));
  }

  static class Fixture {
    public final String statement;
    public final Relation relation;
    public final PredicativeExpression condition;

    public Fixture(String stmt, Relation relation, PredicativeExpression condition) {
      this.statement = stmt;
      this.relation = relation;
      this.condition = condition;
    }

    public DeleteStatement buildStatement() {
      DeleteStatement stmt = new DeleteStatement();
      stmt.setRelation(new RelationSource(relation));
      stmt.setCondition(condition);
      return stmt;
    }
  }
}
