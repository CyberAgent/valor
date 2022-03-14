package jp.co.cyberagent.valor.sdk;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.Murmur3SaltFormatter;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.Query;
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

public class TestParallelMergeScan {
  private static final String RELATION_ID = "test_client";
  private static final String SCHEMA_ID = "s1";

  private static final String KEY_ATTR = "keyAttr";
  private static final String VAL_ATTR = "valAttr";

  private static final AttributeNameExpression KEY1
      = new AttributeNameExpression(KEY_ATTR, IntegerAttributeType.INSTANCE);
  private static final AttributeNameExpression VAL
      = new AttributeNameExpression(VAL_ATTR, StringAttributeType.INSTANCE);

  private static Relation relation;
  private static EmbeddedEKVStorage storage;

  private static ValorContext context;

  @BeforeAll
  public static void init() throws Exception {
    ValorConf conf = new ValorConfImpl();
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, SingletonStubPlugin.NAME);

    relation = ImmutableRelation.builder().relationId(RELATION_ID)
        .addAttribute(KEY_ATTR, true, IntegerAttributeType.INSTANCE)
        .addAttribute(VAL_ATTR, false, StringAttributeType.INSTANCE)
        .build();

    Map<String, Object> saltConfig = new HashMap<>();
    saltConfig.put(Murmur3SaltFormatter.RANGE_PROPKEY, 4);
    saltConfig.put(Murmur3SaltFormatter.ATTRIBUTES_NAME_PROPKEY, Arrays.asList(KEY_ATTR));

    SchemaDescriptor schemaDescriptor1 =
        ImmutableSchemaDescriptor.builder()
            .schemaId(SCHEMA_ID)
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(
                new Murmur3SaltFormatter(saltConfig),
                new AttributeValueFormatter(KEY_ATTR)))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter("")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter(VAL_ATTR)))
            .build();

    storage = new EmbeddedEKVStorage(conf);
    context = StandardContextFactory.create(conf);

    try (ValorConnection conn = ValorConnectionFactory.create(context)){
      conn.createRelation(relation, true);
      conn.createSchema(relation.getRelationId(), schemaDescriptor1, true);
      for (int i = 0; i < 512; i++) {
        Tuple t = new TupleImpl(relation);
        t.setAttribute(KEY_ATTR, i);
        t.setAttribute(VAL_ATTR, Integer.toString(i));
        conn.insert(RELATION_ID, t);
      }
    }
  }

  @SuppressWarnings(value = "unchecked")
  @Test
  public void testSelect() throws Exception {
    Query query = Query.builder()
        .setRelationName(null, relation)
        .setCondition(new LessthanorequalOperator(KEY_ATTR, IntegerAttributeType.INSTANCE, 513))
        .build();
    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      List<Tuple> result = conn.select(query);
      for (int i = 0; i < 512; i++) {
        Tuple t = result.get(i);
        assertThat("key of " + i + " th element unmatched",
            t.getAttribute(KEY_ATTR), equalTo(i));
        assertThat("value of " + i + " th element unmatched",
            t.getAttribute(VAL_ATTR), equalTo(Integer.toString(i)));
      }
    }
  }
}
