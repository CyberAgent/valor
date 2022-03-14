package jp.co.cyberagent.valor.sdk;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.metadata.CompositeSchemaRepository;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.metadata.SchemaRepositoryBase;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestCompositeRepository {

  private static final String RELATION_ID = "r";
  private static final String SCHEMA1_ID = "s1";

  private static final String KEY_ATTR = "keyAttr";
  private static final String COL_ATTR = "colAttr";
  private static final String VAL_ATTR = "valAttr";

  private static final AttributeNameExpression KEY
      = new AttributeNameExpression(KEY_ATTR, StringAttributeType.INSTANCE);
  private static final AttributeNameExpression COL
      = new AttributeNameExpression(COL_ATTR, StringAttributeType.INSTANCE);
  private static final AttributeNameExpression VAL
      = new AttributeNameExpression(VAL_ATTR, IntegerAttributeType.INSTANCE);

  private static Relation relation;
  private static File confDir;
  private static ValorContext context;

  private static final String secondaryNamespace = "secondary";

  @BeforeAll
  public static void init() throws Exception {
    String tmpDir = System.getProperty("java.io.tmpdir");
    confDir = new File(tmpDir, TestCompositeRepository.class.getName());
    confDir.mkdirs();

    ObjectMapper om = new ObjectMapper();
    Map<String, String> defaultConf = new HashMap<>();
    defaultConf.put(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, InMemorySchemaRepository.NAME);
    String defaultPath = confDir.getAbsolutePath() + File.separator + "default.json";
    try (FileOutputStream fos = new FileOutputStream(defaultPath)) {
      om.writeValue(fos, defaultConf);
    }

    Map<String, String> secondaryConf = new HashMap<>();
    secondaryConf.put(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, InMemorySchemaRepository.NAME);
    secondaryConf.put(SchemaRepositoryBase.SCHEMA_REPOSITORY_CACHE_TTL, "100");
    String secondaryPath = confDir.getAbsolutePath() + File.separator + secondaryNamespace + ".json";
    try (FileOutputStream fos = new FileOutputStream(secondaryPath)) {
      om.writeValue(fos, secondaryConf);
    }


    ValorConf conf = new ValorConfImpl();
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, CompositeSchemaRepository.NAME);
    conf.set(CompositeSchemaRepository.CONF_DIR.name, confDir.getAbsolutePath());

    relation = ImmutableRelation.builder().relationId(RELATION_ID)
        .addAttribute(KEY_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(COL_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(VAL_ATTR, false, IntegerAttributeType.INSTANCE)
        .build();

    SchemaDescriptor schemaDescriptor1 =
        ImmutableSchemaDescriptor.builder()
            .schemaId(SCHEMA1_ID)
            .storageClassName(SingletonStubPlugin.NAME)
            .storageConf(conf)
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(new AttributeValueFormatter(KEY_ATTR)))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(new AttributeValueFormatter(COL_ATTR)))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter(VAL_ATTR)))
            .build();
    context = StandardContextFactory.create(conf);

    try (ValorConnection conn = ValorConnectionFactory.create(context)){
      conn.createRelation(relation, true);
      conn.createSchema(relation.getRelationId(), schemaDescriptor1, true);
    }
  }

  @AfterAll
  public static void tearDown() throws Exception {
    for (File f : confDir.listFiles()) {
      f.delete();
    }
    confDir.delete();
  }


  @SuppressWarnings(value = "unchecked")
  @Test
  public void test() throws Exception {

    Tuple t1 = new TupleImpl(relation);
    t1.setAttribute(KEY_ATTR, "a1");
    t1.setAttribute(COL_ATTR, "p");
    t1.setAttribute(VAL_ATTR, 100);


    final Relation relInSecondary = ImmutableRelation.builder().relationId(RELATION_ID)
        .addAttribute(KEY_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(VAL_ATTR, false, IntegerAttributeType.INSTANCE)
        .build();

    SchemaDescriptor schemaInSecondary =
        ImmutableSchemaDescriptor.builder()
            .schemaId(SCHEMA1_ID)
            .storageClassName(SingletonStubPlugin.NAME)
            .storageConf(context.getConf())
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(new AttributeValueFormatter(KEY_ATTR)))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter("s")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter(VAL_ATTR)))
            .build();

    Tuple t2 = new TupleImpl(relInSecondary);
    t2.setAttribute(KEY_ATTR, "a2");
    t2.setAttribute(VAL_ATTR, 200);


    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.createRelation(secondaryNamespace, relInSecondary, false);
      conn.createSchema(secondaryNamespace, RELATION_ID, schemaInSecondary, false);

      // insert
      conn.insert(RELATION_ID, Arrays.asList(t1));
      conn.insert(secondaryNamespace, RELATION_ID, Arrays.asList(t2));

      // query
      RelationScan q1 = new RelationScan.Builder()
          .setItems(KEY, COL, VAL).setRelationSource(null, relation).build();
      List<Tuple> result = conn.scan(q1);
      assertThat(result, hasSize(2));
      Tuple r = result.get(0);
      assertThat(r.getAttribute(KEY_ATTR), equalTo("a1"));
      assertThat(r.getAttribute(COL_ATTR), equalTo("p"));
      assertThat(r.getAttribute(VAL_ATTR), equalTo(100));
      r = result.get(1);
      assertThat(r.getAttribute(KEY_ATTR), equalTo("a2"));
      assertThat(r.getAttribute(COL_ATTR), equalTo("s"));
      assertThat(r.getAttribute(VAL_ATTR), equalTo(200));

      RelationScan q2 = new RelationScan.Builder()
          .setItems(KEY, VAL).setRelationSource(secondaryNamespace, relInSecondary).build();
      result = conn.scan(q2);
      assertThat(result, hasSize(1));
      assertThat(r.getAttribute(KEY_ATTR), equalTo("a2"));
      assertThat(r.getAttribute(VAL_ATTR), equalTo(200));
    }
  }
}
