package jp.co.cyberagent.valor.sdk.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import jp.co.cyberagent.valor.sdk.SingletonStubPlugin;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.JsonFormatter;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestJsonUnnestSchema {

  private static ValorConf conf;
  private static ValorContext context;

  private static final String W_REL_ID = "write_rel";
  private static final String W_KEY_ATTR = "k1";
  private static final String W_VAL_ATTR = "v1";
  private static Relation writeRelation;

  @BeforeAll
  public static void init() throws Exception {
    conf = new ValorConfImpl();
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, SingletonStubPlugin.NAME);
    context = StandardContextFactory.create(conf);

    writeRelation = ImmutableRelation.builder()
        .relationId(W_REL_ID)
        .addAttribute(W_KEY_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(W_VAL_ATTR, false, StringAttributeType.INSTANCE)
        .build();
    SchemaDescriptor ws1 = ImmutableSchemaDescriptor.builder()
        .schemaId("s1")
        .storageClassName(SingletonStubPlugin.NAME)
        .storageConf(conf)
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(new AttributeValueFormatter(W_KEY_ATTR)))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter(W_REL_ID)))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter(W_VAL_ATTR)))
        .build();


    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.createRelation(writeRelation, true);
      conn.createSchema(writeRelation.getRelationId(), ws1, true);


    }
  }

  @SuppressWarnings(value = "unchecked")
  @Test
  public void testRootUnnestAndREname() throws Exception {
    final String R_REL_ID = "r_rootunnest_rel_id";
    final String R_KEY_ATTR = "k1";
    final String R_G1_ATTR = "g1";
    final String R_G2_ATTR = "g2";

    Relation relation = ImmutableRelation.builder()
        .relationId(R_REL_ID)
        .addAttribute(R_KEY_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(R_G1_ATTR, false, StringAttributeType.INSTANCE)
        .addAttribute(R_G2_ATTR, false, IntegerAttributeType.INSTANCE)
        .build();
    Map jsonFmtConf = new HashMap(){
      {
        put("root", "f2");
        put("unnestRoot", true);
        put("attrs", Arrays.asList(R_G1_ATTR, R_G2_ATTR));
        put("rename", new HashMap<String, String>(){
          {
            put("h2", "g2");
          }
        });
      }
    };
    SchemaDescriptor rs1 = ImmutableSchemaDescriptor.builder()
        .schemaId("s1")
        .storageClassName(SingletonStubPlugin.NAME)
        .storageConf(conf)
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(new AttributeValueFormatter(R_KEY_ATTR)))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter(W_REL_ID)))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new JsonFormatter(jsonFmtConf)))
        .build();


    try (ValorConnection conn = ValorConnectionFactory.create(context)){
      Tuple t = new TupleImpl(writeRelation);
      t.setAttribute("k1", R_REL_ID);
      t.setAttribute("v1", "{\"f1\":\"test\", \"f2\":[{\"g1\":\"x\",\"h2\":0},{\"g1\":\"x\",\"h2\":1}]}");
      conn.insert(W_REL_ID, t);

      conn.createRelation(relation, true);
      conn.createSchema(R_REL_ID, rs1, true);
      PredicativeExpression query
          = new EqualOperator(R_KEY_ATTR, StringAttributeType.INSTANCE, R_REL_ID);
      List<Tuple> result = conn.select(R_REL_ID, relation.getAttributeNames(), query);
      assertThat(result, hasSize(2));
      IntStream.range(0, 2).forEach(i -> {
        assertThat(result.get(i).getAttribute(R_G1_ATTR), equalTo("x"));
        assertThat(result.get(i).getAttribute(R_G2_ATTR), equalTo(i));
      });

    }
  }

  @Test
  public void testUnnest() throws Exception {
    final String R_REL_ID = "r_rel_id";
    final String R_KEY_ATTR = "k1";
    final String R_VAL_ATTR = "f1";
    final String R_NEST_ATTR = "n1";

    Relation relation = ImmutableRelation.builder()
        .relationId(R_REL_ID)
        .addAttribute(R_KEY_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(R_VAL_ATTR, false, StringAttributeType.INSTANCE)
        .addAttribute(R_NEST_ATTR, false, IntegerAttributeType.INSTANCE)
        .build();
    Map nestConf = new HashMap() {
      {
        put("f2", R_NEST_ATTR);
      }
    };
    Map jsonFmtConf = new HashMap(){
      {
        put("attrs", Arrays.asList(R_VAL_ATTR));
        put("unnest", nestConf);
      }
    };
    SchemaDescriptor rs1 = ImmutableSchemaDescriptor.builder()
        .schemaId("s1")
        .storageClassName(SingletonStubPlugin.NAME)
        .storageConf(conf)
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(new AttributeValueFormatter(R_KEY_ATTR)))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter(W_REL_ID)))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new JsonFormatter(jsonFmtConf)))
        .build();


    try (ValorConnection conn = ValorConnectionFactory.create(context)){
      Tuple t = new TupleImpl(writeRelation);
      t.setAttribute("k1", R_REL_ID);
      t.setAttribute("v1", "{\"f1\":\"test\", \"f2\":[0,1,2]}");
      conn.insert(W_REL_ID, t);

      conn.createRelation(relation, true);
      conn.createSchema(R_REL_ID, rs1, true);
      PredicativeExpression query
          = new EqualOperator(R_KEY_ATTR, StringAttributeType.INSTANCE, R_REL_ID);
      List<Tuple> result = conn.select(R_REL_ID, relation.getAttributeNames(), query);
      assertThat(result, hasSize(3));
      IntStream.range(0, 3).forEach(i -> {
        assertThat(result.get(i).getAttribute(R_VAL_ATTR), equalTo("test"));
        assertThat(result.get(i).getAttribute(R_NEST_ATTR), equalTo(i));
      });

    }
  }

}
