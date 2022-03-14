package jp.co.cyberagent.valor.ql.parse;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.InsertStatement;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;
import jp.co.cyberagent.valor.spi.plan.model.RelationSource;
import jp.co.cyberagent.valor.spi.plan.model.ValueItem;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestParseInsertStatement {

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
        new Fixture("INSERT INTO relId VALUES ('key', 100)",
            relation, Arrays.asList("key", 100))
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
    public final List<List<Object>> values;

    public Fixture(String stmt, Relation relation, List<Object>... values) {
      this.statement = stmt;
      this.relation = relation;
      this.values = Arrays.asList(values);
    }

    public InsertStatement buildStatement() {
      InsertStatement stmt = new InsertStatement();
      stmt.setRelation(new RelationSource(relation));
      values.forEach(vs -> {
        ValueItem item = new ValueItem();
        vs.forEach(v -> item.addValue(new ConstantExpression(v)));
        stmt.addValue(item);
      });
      return stmt;
    }

  }
}
