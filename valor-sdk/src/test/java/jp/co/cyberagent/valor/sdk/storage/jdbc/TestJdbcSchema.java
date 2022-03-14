package jp.co.cyberagent.valor.sdk.storage.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestJdbcSchema {

  private static final String RELATION_ID = "testTuple";
  private static final String KV_SCHEMA_ID = "jdbcSchema";
  private static final String TABLE_NAME = "DERBYTABLE";

  // TODO support case sensitive attribute names
  private static final String KEY1 = "K1";
  private static final String KEY2 = "K2";
  private static final String ATTR1 = "A1";
  private static final String ATTR2 = "A2";

  private static final String DDL = String.format("create table %s(%s varchar(10),%s varchar(10)," +
          "%s varchar(10),%s varchar(10), primary key (%s, %s))", TABLE_NAME, KEY1, KEY2, ATTR1,
      ATTR2, KEY1, KEY2);

  private static ValorContext context;
  private static SchemaRepository repository;

  private static Relation relation;
  private static Schema schema;
  private static Storage storage;

  private static String jdbcUrl;

  @SuppressWarnings("resource")
  @BeforeAll
  public static void setup() throws Exception {

    String tmpDir = System.getProperty("java.io.tmpdir");
    jdbcUrl = String.format("jdbc:derby:%s/testjdbcstorage%s", tmpDir, System.currentTimeMillis());

    ValorConf conf = new ValorConfImpl();
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, InMemorySchemaRepository.NAME);
    conf.set(JdbcStorage.JDBC_URL.name, jdbcUrl);
    conf.set(JdbcStorage.JDBC_TABLE_NAME.name, TABLE_NAME);
    context = StandardContextFactory.create(conf);
    context.installPlugin(new JdbcPlugin());
    repository = context.createRepository(conf);

    try (Connection conn = DriverManager.getConnection(jdbcUrl + ";create=true"); Statement stmt
        = conn.createStatement(); ResultSet rs = conn.getMetaData().getTables(null, null,
        TABLE_NAME, null);) {
      if (!rs.next()) {
        stmt.executeUpdate(DDL);
      }
    }

    relation = ImmutableRelation.builder().relationId(RELATION_ID).addAttribute(KEY1, true,
        StringAttributeType.INSTANCE).addAttribute(KEY2, true, StringAttributeType.INSTANCE)
        .addAttribute(ATTR1, false, StringAttributeType.INSTANCE)
        .addAttribute(ATTR2, false, StringAttributeType.INSTANCE).build();
    SchemaDescriptor descriptor =
        ImmutableSchemaDescriptor.builder().isPrimary(true)
            .schemaId(KV_SCHEMA_ID)
            .storageClassName(JdbcStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(KEY1, Arrays.asList(AttributeValueFormatter.create(KEY1)))
            .addField(KEY2, Arrays.asList(AttributeValueFormatter.create(KEY2)))
            .addField(ATTR1, Arrays.asList(AttributeValueFormatter.create(ATTR1)))
            .addField(ATTR2, Arrays.asList(AttributeValueFormatter.create(ATTR2)))
            .build();
    storage = new JdbcStorage(conf);
    schema = storage.buildSchema(relation, descriptor);

    repository.createRelation(relation, true);
    repository.createSchema(relation.getRelationId(), descriptor, true);
  }

  @AfterAll
  public static void tearDown() throws Exception {
  }

  @Test
  public void test() throws Exception {
    String relationId = relation.getRelationId();
    Tuple current1 = buildTuple("k1", "l1", "v1", "w1");
    Tuple current2 = buildTuple("k2", "l2", "v2", "w2");
    try (ValorConnection client = ValorConnectionFactory.create(context)) {
      client.insert(relationId, current1, current2);
    }
    try (Connection conn = DriverManager.getConnection(jdbcUrl); Statement stmt =
        conn.createStatement();) {
      try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
        assertThat(rs.next(), is(true));
        assertThat(rs.getString(KEY1), equalTo("k1"));
        assertThat(rs.getString(KEY2), equalTo("l1"));
        assertThat(rs.getString(ATTR1), equalTo("v1"));
        assertThat(rs.getString(ATTR2), equalTo("w1"));
        assertThat(rs.next(), is(true));
        assertThat(rs.getString(KEY1), equalTo("k2"));
        assertThat(rs.getString(KEY2), equalTo("l2"));
        assertThat(rs.getString(ATTR1), equalTo("v2"));
        assertThat(rs.getString(ATTR2), equalTo("w2"));
        assertThat(rs.next(), is(false));
      }
    }

    PredicativeExpression conditions = new EqualOperator(new AttributeNameExpression(KEY1,
        StringAttributeType.INSTANCE), new ConstantExpression(current1.getAttribute(KEY1)));
    List<Tuple> selectResult;
    try (ValorConnection client = ValorConnectionFactory.create(context)) {
      selectResult = client.select(relationId, relation.getAttributeNames(), conditions);
    }

    assertThat(selectResult, hasSize(1));
    Tuple t = selectResult.get(0);
    assertThat(t.getAttribute(KEY1), equalTo("k1"));
    assertThat(t.getAttribute(KEY2), equalTo("l1"));
    assertThat(t.getAttribute(ATTR1), equalTo("v1"));
    assertThat(t.getAttribute(ATTR2), equalTo("w1"));

    // TODO implement update
    /*
    Map<String, Object> newVal = Maps.newHashMap();
    newVal.put(ATTR1, "va");
    client.update(newVal, conditions);

    selectResult = client.select(relation.getAttributeNames(), conditions);
    assertThat(selectResult, hasSize(1));
    t = selectResult.get(0);
    assertThat(t.getAttribute(KEY1), equalTo("k1"));
    assertThat(t.getAttribute(KEY2), equalTo("l1"));
    assertThat(t.getAttribute(ATTR1), equalTo("va"));
    assertThat(t.getAttribute(ATTR2), equalTo("w1"));

    conditions = new EqualOperator(new AttributeNameExpression(KEY1, StringAttributeType
    .INSTANCE), new ConstantExpression(current1.getAttribute("k2")));
    selectResult = client.select(relation.getAttributeNames(), conditions);
    assertThat(selectResult, hasSize(1));
    t = selectResult.get(0);
    assertThat(t.getAttribute(KEY1), equalTo("k2"));
    assertThat(t.getAttribute(KEY2), equalTo("l2"));
    assertThat(t.getAttribute(ATTR1), equalTo("v2"));
    assertThat(t.getAttribute(ATTR2), equalTo("w2"));
    */

  }

  private Tuple buildTuple(String k1, String k2, String a1, String a2) {
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(KEY1, k1);
    tuple.setAttribute(KEY2, k2);
    tuple.setAttribute(ATTR1, a1);
    if (a2 != null) {
      tuple.setAttribute(ATTR2, a2);
    }
    return tuple;
  }
}
