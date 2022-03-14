package jp.co.cyberagent.valor.sdk.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.sdk.SingletonStubPlugin;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.plan.JoinablePlanner;
import jp.co.cyberagent.valor.sdk.plan.MergeJoinPlan;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.plan.QueryPlan;
import jp.co.cyberagent.valor.spi.plan.model.Query;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.Test;

public class TestJoinSchema {

  private ValorConf conf;
  private ValorContext context;

  public TestJoinSchema() throws Exception {
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
    SchemaDescriptor s1 = ImmutableSchemaDescriptor.builder().schemaId("s1")
        .storageClassName(SingletonStubPlugin.NAME)
        .storageConf(conf)
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(new ConstantFormatter("s1")))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(new AttributeValueFormatter("k1")))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter("v1")))
        .build();
    SchemaDescriptor s2 = ImmutableSchemaDescriptor.builder().schemaId("s2")
        .storageClassName(SingletonStubPlugin.NAME)
        .storageConf(conf)
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(new ConstantFormatter("s2")))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(new AttributeValueFormatter("k1")))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter("v2")))
        .build();

    try (ValorConnection conn = ValorConnectionFactory.create(context)){
      conn.createRelation(relation, true);
      conn.createSchema(relation.getRelationId(), s1, true);
      conn.createSchema(relation.getRelationId(), s2, true);

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

  }
}
