package jp.co.cyberagent.valor.sdk.schema.kv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import java.util.Arrays;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.plan.SimplePlan;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.Query;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.NumberAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.Test;

public class TestChooseSchema {

  @Test
  public void testInsertAndDelete() throws Exception {
    final String REL_ID = "r";
    final String SCHEMA1_ID = "s1";
    final String SCHEMA2_ID = "s2";
    final String KEY1_ATTR = "k1";
    final String KEY2_ATTR = "k2";
    final String VAL_ATTR = "v";

    final AttributeNameExpression k1
        = new AttributeNameExpression(KEY1_ATTR, StringAttributeType.INSTANCE);
    final AttributeNameExpression k2
        = new AttributeNameExpression(KEY2_ATTR, StringAttributeType.INSTANCE);
    final AttributeNameExpression v
        = new AttributeNameExpression(VAL_ATTR, StringAttributeType.INSTANCE);

    Relation relation = ImmutableRelation.builder().relationId(REL_ID)
        .addAttribute(KEY1_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(KEY2_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(VAL_ATTR, false, NumberAttributeType.INSTANCE)
        .build();

    ValorConf conf = new ValorConfImpl();
    SchemaDescriptor schemaDescriptor1 =
        ImmutableSchemaDescriptor.builder()
            .schemaId(SCHEMA1_ID)
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(
                VintSizePrefixHolder.create(new AttributeValueFormatter(KEY1_ATTR)),
                new AttributeValueFormatter(KEY2_ATTR))
            )
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
                new AttributeValueFormatter(KEY1_ATTR))
            )
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter("")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter(VAL_ATTR)))
            .build();

    try (ValorConnection conn = ValorConnectionFactory.create(StandardContextFactory.create())) {
      conn.createRelation(relation, true);
      conn.createSchema(REL_ID, schemaDescriptor1, true);
      conn.createSchema(REL_ID, schemaDescriptor2, true);

      Query q = Query.builder()
          .addItems(k1, k2, v)
          .setRelationName(null, relation)
          .setCondition(new EqualOperator(k1, new ConstantExpression("v")))
          .build();
      SimplePlan p = (SimplePlan) conn.plan(q);
      Schema s = p.getScan().getSchema();
      assertThat(s.getSchemaId(), equalTo(SCHEMA1_ID));

      q = Query.builder()
          .addItems(k1, k2, v)
          .setRelationName(null, relation)
          .setCondition(new EqualOperator(k2, new ConstantExpression("v")))
          .build();
      p = (SimplePlan) conn.plan(q);
      s = p.getScan().getSchema();
      assertThat(s.getSchemaId(), equalTo(SCHEMA2_ID));

    }
  }
}
