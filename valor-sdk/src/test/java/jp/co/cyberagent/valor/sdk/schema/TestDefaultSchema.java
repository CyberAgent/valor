package jp.co.cyberagent.valor.sdk.schema;

import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.plan.JoinablePlanner;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.Test;

public class TestDefaultSchema {

  private ValorConf conf;
  private ValorContext context;

  public TestDefaultSchema() throws Exception {
    conf = new ValorConfImpl();
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, InMemorySchemaRepository.NAME);
    conf.set(ValorContext.PLANNER_CLASS_KEY, JoinablePlanner.NAME);
    context = StandardContextFactory.create(conf);
  }

  @SuppressWarnings(value = "unchecked")
  @Test
  public void testJoinBySingleKey() throws Exception {
    String relId = "jbs";
    Relation relation = ImmutableRelation.builder().relationId(relId)
        .addAttribute("k1", true, StringAttributeType.INSTANCE)
        .addAttribute("v1", false, IntegerAttributeType.INSTANCE)
        .addAttribute("v2", false, IntegerAttributeType.INSTANCE)
        .build();

    /*
    try (ValorConnection conn = ValorConnectionFactory.create(context)){
      conn.createRelation(relation, true);

      Tuple t = new TupleImpl(relation);
      t.setAttribute("k1", "a");
      t.setAttribute("v1", 1);
      t.setAttribute("v2", 2);

      conn.insert(relId, t);

      Query query = Query.builder()
          .setRelationName(null, relation)
          .setCondition(new EqualOperator("k1", StringAttributeType.INSTANCE, "a"))
          .build();

      QueryPlan plan = conn.plan(query);
      assertThat(plan, is(instanceOf(MergeJoinPlan.class)));
      List<Tuple> result = conn.select(query);
      assertThat(result, hasSize(1));
      Tuple a = result.get(0);
      assertThat(a.getAttribute("k1"), equalTo("a"));
      assertThat(a.getAttribute("v1"), equalTo(1));
      assertThat(a.getAttribute("v2"), equalTo(2));
    }
*/
  }
}
