package jp.co.cyberagent.valor.sdk.schema.kv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeNamesFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeValuesFormatter;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.Test;

public class TestMultiKVSchema {

  private static final String REL_ID = "r";
  private static final String KEY_ATTR = "k";
  private static final String VAL1_ATTR = "v";
  private static final String VAL2_ATTR = "w";

  private final ValorContext context;

  private Relation relation;

  public TestMultiKVSchema() throws Exception {

    ValorConf conf = new ValorConfImpl();
    conf.set("testName", this.getClass().getCanonicalName());
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, InMemorySchemaRepository.NAME);
    context = StandardContextFactory.create(conf);

    relation= ImmutableRelation.builder().relationId(REL_ID)
        .addAttribute(KEY_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(VAL1_ATTR, false, StringAttributeType.INSTANCE)
        .addAttribute(VAL2_ATTR, false, StringAttributeType.INSTANCE)
        .build();

    SchemaDescriptor schemaDescriptor = ImmutableSchemaDescriptor.builder()
        .schemaId("test")
        .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
        .storageConf(new ValorConfImpl())
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(new AttributeValueFormatter(KEY_ATTR)))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(MultiAttributeNamesFormatter.create(KEY_ATTR)))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(MultiAttributeValuesFormatter.create(KEY_ATTR)))
        .build();

    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.createRelation(relation, true);
      conn.createSchema(REL_ID, schemaDescriptor, true);
    }

  }

  @Test
  public void test() throws Exception {

    Tuple t1 = new TupleImpl(this.relation);
    t1.setAttribute(KEY_ATTR, "k1");
    t1.setAttribute(VAL1_ATTR, "v1");
    t1.setAttribute(VAL2_ATTR, "w1");

    Tuple t2 = new TupleImpl(this.relation);
    t2.setAttribute(KEY_ATTR, "k2");
    t2.setAttribute(VAL1_ATTR, "v2");

    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.insert(REL_ID, t1, t2);
      List<Tuple> records = conn.select(REL_ID, relation.getAttributeNames(), null);
      assertThat(records, hasSize(2));
    }

  }

}
