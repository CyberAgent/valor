package jp.co.cyberagent.valor.sdk;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionClause;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.plan.model.RelationSource;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.Test;

public class TestValorConnection {

  private static final String RELATION_ID = "test_client";
  private static final String SCHEMA1_ID = "s1";
  private static final String SCHEMA2_ID = "s2";

  private static final String KEY1_ATTR = "keyAttr1";
  private static final String KEY2_ATTR = "keyAttr2";
  private static final String VAL_ATTR = "valAttr";

  private static final AttributeNameExpression KEY1
      = new AttributeNameExpression(KEY1_ATTR, StringAttributeType.INSTANCE);
  private static final AttributeNameExpression KEY2
      = new AttributeNameExpression(KEY2_ATTR, StringAttributeType.INSTANCE);
  private static final AttributeNameExpression VAL
      = new AttributeNameExpression(VAL_ATTR, IntegerAttributeType.INSTANCE);

  private final Relation relation;
  private EmbeddedEKVStorage storage;

  private ValorContext context;

  public TestValorConnection() throws Exception {
    ValorConf conf = new ValorConfImpl();
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, InMemorySchemaRepository.NAME);

    relation = ImmutableRelation.builder().relationId(RELATION_ID)
        .addAttribute(KEY1_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(KEY2_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(VAL_ATTR, false, IntegerAttributeType.INSTANCE)
        .build();

    SchemaDescriptor schemaDescriptor1 =
        ImmutableSchemaDescriptor.builder()
            .schemaId(SCHEMA1_ID)
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(
                VintSizePrefixHolder.create(new AttributeValueFormatter(KEY1_ATTR)),
                new AttributeValueFormatter(KEY2_ATTR)))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter("")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter(VAL_ATTR)))
            .build();
    SchemaDescriptor schemaDescriptor2 =
        ImmutableSchemaDescriptor.builder()
            .schemaId(SCHEMA2_ID)
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(
                VintSizePrefixHolder.create(new AttributeValueFormatter(KEY2_ATTR)),
                new AttributeValueFormatter(KEY1_ATTR)))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter("")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter(VAL_ATTR)))
            .build();
    storage = new EmbeddedEKVStorage(conf);

    context = StandardContextFactory.create(conf);

    try (ValorConnection conn = ValorConnectionFactory.create(context)){
      conn.createRelation(relation, true);
      conn.createSchema(relation.getRelationId(), schemaDescriptor1, true);
      conn.createSchema(relation.getRelationId(), schemaDescriptor2, true);
    }
  }

  @SuppressWarnings(value = "unchecked")
  @Test
  public void testCRUD() throws Exception {
    Tuple t1 = new TupleImpl(relation);
    t1.setAttribute(KEY1_ATTR, "a1");
    t1.setAttribute(KEY2_ATTR, "a2");
    t1.setAttribute(VAL_ATTR, 100);
    Tuple t2 = new TupleImpl(relation);
    t2.setAttribute(KEY1_ATTR, "b1");
    t2.setAttribute(KEY2_ATTR, "b2");
    t2.setAttribute(VAL_ATTR, 200);
    ProjectionClause items = new ProjectionClause(KEY1, KEY2, VAL);
    RelationSource source = new RelationSource(relation);
    PredicativeExpression
        condition = new EqualOperator(KEY1_ATTR, StringAttributeType.INSTANCE, "a1");

    RelationScan query = new RelationScan(items, source, condition);
    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      List<Tuple> result = conn.scan(query);
      assertThat(result, hasSize(0));
      // create
      conn.insert(RELATION_ID, t1, t2);
      // read
      result = conn.scan(query);
      assertThat(result, hasSize(1));
      assertThat(result.get(0).getAttribute(VAL_ATTR), equalTo(100));

      // update
      Map<String, Object> newValue = new HashMap<>();
      newValue.put(VAL_ATTR, 101);
      conn.update(RELATION_ID, newValue, query.getCondition());
      result = conn.scan(query);
      assertThat(result, hasSize(1));
      assertThat(result.get(0).getAttribute(VAL_ATTR), equalTo(101));

      // update
      conn.delete(RELATION_ID, query.getCondition());
      result = conn.scan(query);
      assertThat(result, hasSize(0));
    }
  }
}
