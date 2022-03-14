package jp.co.cyberagent.valor.hbase.schemas;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import jp.co.cyberagent.valor.hbase.HBasePlugin;
import jp.co.cyberagent.valor.hbase.repository.HBaseSchemaRepository;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.hbase.storage.HBaseConnection;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.hbase.util.AssertSchema;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeNamesFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeValuesFormatter;
import jp.co.cyberagent.valor.sdk.holder.SuffixHolder;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.io.StorageConnectionWrapper;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.RegexpOperator;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedMonoKeyValueSchema;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedMultiKeyValuesSchema;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.InvalidOperationException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.zookeeper.repository.ZookeeperSchemaRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSingleTableSchema {

  private static final String RELATION_ID = "testTuple";

  private static final String KV_SCHEMA_ID = "kvSchema";
  private static final String KV_SCHEMA_TABLE = "kvTable";
  private static final String KV_SCHEMA_FAMILY = "kvFamily";

  private static final String ROW_SCHEMA_ID = "rowSchema";
  private static final String ROW_SCHEMA_TABLE = "rowTable";
  private static final String ROW_SCHEMA_FAMILY = "d2";

  private static final String KEY1 = "k1";
  private static final String KEY2 = "k2";
  private static final String ATTR1 = "a1";
  private static final String ATTR2 = "a2";

  private static HBaseTestingUtility utility;

  private static SchemaRepository repository;
  private static Connection conn;

  private static Relation relation;
  private static SortedMonoKeyValueSchema kvSchema;
  private static SortedMultiKeyValuesSchema rowSchema;
  private static Storage storage;

  private static ValorContext context;

  private static Function<StorageConnection, HBaseConnection> cast = (c) -> {
    if (c instanceof StorageConnectionWrapper) {
      return (HBaseConnection) ((StorageConnectionWrapper) c).getWrapped();
    } else {
      return (HBaseConnection)c;
    }
  };



  @SuppressWarnings("resource")
  @BeforeAll
  public static void setup() throws Exception {

    utility = new HBaseTestingUtility();
    utility.cleanupTestDir();
    utility.startMiniCluster(1);
    utility.createTable(Bytes.toBytes(HBaseSchemaRepository.SCHEMA_TABLE.defaultValue),
        Bytes.toBytes(HBaseSchemaRepository.SCHEMA_FAMILY.defaultValue));
    Configuration clusterConf = utility.getConfiguration();

    String zkQuorum = clusterConf.get(HConstants.ZOOKEEPER_QUORUM);
    String zkQuorumPort = clusterConf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
    ValorConf conf = new ValorConfImpl();
    conf.set(StandardContextFactory.SCHEMA_REPOSITORY_CLASS.name, HBaseSchemaRepository.NAME);
    conf.set(ZookeeperSchemaRepository.SCHEMA_REPOS_ZKQUORUM.name, zkQuorum + ":" + zkQuorumPort);
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum + ":" + zkQuorumPort);

    context = StandardContextFactory.create(conf);
    context.installPlugin(new HBasePlugin());
    repository = context.createRepository(conf);

    byte[][] kvSchemaFamilies = {Bytes.toBytes(KV_SCHEMA_FAMILY)};
    byte[][] splitKeys = {Bytes.toBytes(0x10)};
    utility.createTable(Bytes.toBytes(KV_SCHEMA_TABLE), kvSchemaFamilies, splitKeys);
    utility.createTable(Bytes.toBytes(ROW_SCHEMA_TABLE), Bytes.toBytes(ROW_SCHEMA_FAMILY));

    relation = ImmutableRelation.builder().relationId(RELATION_ID)
        .addAttribute(KEY1, true,  StringAttributeType.INSTANCE)
        .addAttribute(KEY2, true, StringAttributeType.INSTANCE)
        .addAttribute(ATTR1, false, StringAttributeType.INSTANCE)
        .addAttribute(ATTR2, false, StringAttributeType.INSTANCE).build();
    SchemaDescriptor kvDescriptor =
        ImmutableSchemaDescriptor.builder().isPrimary(true)
            .schemaId(KV_SCHEMA_ID)
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(conf)
            .conf(conf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create(KV_SCHEMA_TABLE)))
            .addField(HBaseCell.ROWKEY, Arrays.asList(AttributeValueFormatter.create(KEY1)))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create(KV_SCHEMA_FAMILY)))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(AttributeValueFormatter.create(KEY2)))
            .addField(HBaseCell.VALUE, Arrays.asList(
                    SuffixHolder.create("\t", AttributeValueFormatter.create(ATTR1)),
                    VintSizePrefixHolder.create(AttributeValueFormatter.create(ATTR2))))
            .build();

    storage = context.createStorage(kvDescriptor);
    kvSchema = (SortedMonoKeyValueSchema) storage.buildSchema(relation, kvDescriptor);

    SchemaDescriptor rowDescriptor =
        ImmutableSchemaDescriptor.builder().isPrimary(false)
            .schemaId(ROW_SCHEMA_ID)
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(conf)
            .conf(conf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create(ROW_SCHEMA_TABLE)))
            .addField(HBaseCell.ROWKEY, Arrays.asList(
                SuffixHolder.create("-", AttributeValueFormatter.create(KEY2)),
                AttributeValueFormatter.create(KEY1)))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create(ROW_SCHEMA_FAMILY)))
            .addField(HBaseCell.QUALIFIER,
                Arrays.asList(MultiAttributeNamesFormatter.create(KEY1, KEY2)))
            .addField(HBaseCell.VALUE,
                Arrays.asList(MultiAttributeValuesFormatter.create(KEY1, KEY2)))
            .build();
    rowSchema = (SortedMultiKeyValuesSchema) storage.buildSchema(relation, rowDescriptor);

    repository.createRelation(relation, true);
    repository.createSchema(relation.getRelationId(), kvDescriptor, true);
    repository.createSchema(relation.getRelationId(), rowDescriptor, true);
    conn = ConnectionFactory.createConnection(clusterConf);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    repository.close();
    conn.close();
    utility.shutdownMiniCluster();
  }

  @Test
  public void testSplit() throws Exception {
    FieldComparator comparator = FieldComparator.build(
        FieldComparator.Operator.BETWEEN,
        Bytes.toBytes(""), Bytes.toBytes(0x00), Bytes.toBytes(0x20), null);
    Map<String, FieldComparator> comparators = new HashMap<>();
    comparators.put(HBaseCell.ROWKEY, comparator);
    StorageScan ss = StorageScan.build(Arrays.asList(HBaseCell.ROWKEY), comparators);

    try (ValorConnection valorConn = ValorConnectionFactory.create(context);
         HBaseConnection hbaseConn = cast.apply(valorConn.createConnection(kvSchema))
    ) {
      List<StorageScan> splitted = hbaseConn.split(ss);
      assertEquals(2, splitted.size());
      assertArrayEquals(Bytes.toBytes(0x00), splitted.get(0).getStart(HBaseCell.ROWKEY));
      assertArrayEquals(Bytes.toBytes(0x10), splitted.get(0).getStop(HBaseCell.ROWKEY));
      assertArrayEquals(Bytes.toBytes(0x10), splitted.get(1).getStart(HBaseCell.ROWKEY));
      assertArrayEquals(Bytes.toBytes(0x20), splitted.get(1).getStop(HBaseCell.ROWKEY));
    }
  }

  @Test
  public void testConnectionReused() throws Exception {
    Connection conn = null;
    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      try (HBaseConnection conn1 = cast.apply(valorConn.getContext().connect(kvSchema));
           HBaseConnection conn2 = cast.apply(valorConn.getContext().connect(rowSchema))){
        conn = conn1.getConnection();
        assertTrue(conn == conn2.getConnection());
      };
    }

    // reuse over client and connection
    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      try (HBaseConnection conn3 = cast.apply(valorConn.getContext().connect(kvSchema))){
        assertTrue(conn == conn3.getConnection());
      }
      try (HBaseConnection conn4 = cast.apply(valorConn.getContext().connect(rowSchema))){
        assertTrue(conn == conn4.getConnection());
      }
    }
  }

  @Test
  public void testCRUD() throws Exception {
    String relationId = relation.getRelationId();
    Tuple current1 = buildTuple("testPut_k1", "testPut_k1", "testPut_k1", "testPut_k1");
    Tuple current2 = buildTuple("testPut_k1", "testPut_k1_2", "testPut_k1_2", null);
    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      valorConn.insert(relationId, current1);
      valorConn.insert(relationId, current2);
    }

    AssertSchema.assertHasTuples(conn, KV_SCHEMA_TABLE, kvSchema, current1, current2);
    AssertSchema.assertHasTuples(conn, ROW_SCHEMA_TABLE, rowSchema, current1, current2);

    final PredicativeExpression conditions = new EqualOperator(
        new AttributeNameExpression(KEY1, StringAttributeType.INSTANCE),
        new ConstantExpression(current1.getAttribute(KEY1))
    );
    List<Tuple> selectResult;
    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      selectResult = valorConn.select(relationId, relation.getKeyAttributeNames(), conditions);
    }
    assertEquals(2, selectResult.size());
    Tuple s = selectResult.get(0);
    assertEquals(current1.getAttribute(KEY1), s.getAttribute(KEY1));
    assertEquals(current1.getAttribute(KEY2), s.getAttribute(KEY2));

    Map<String, Object> newVal = Maps.newHashMap();
    newVal.put(ATTR1, "testPut_updated");
    current1.setAttribute(ATTR1, "testPut_updated");
    current2.setAttribute(ATTR1, "testPut_updated");
    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      valorConn.update(relationId, newVal, conditions);
    }
    AssertSchema.assertHasTuples(conn, KV_SCHEMA_TABLE, kvSchema, current1, current2);
    AssertSchema.assertHasTuples(conn, ROW_SCHEMA_TABLE, rowSchema, current1, current2);

    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      selectResult = valorConn.select(relationId, relation.getAttributeNames(), conditions);
    }

    assertEquals(2, selectResult.size());
    for (Tuple s2 : selectResult) {
      assertEquals("testPut_updated", s2.getAttribute(ATTR1));
    }

    newVal = Maps.newHashMap();
    newVal.put(KEY2, "testPut_updated_key");
    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      valorConn.update(relationId, newVal, conditions);
      fail();
    } catch (Exception e) {
      // key attributes is not updatable
      assertTrue(e instanceof InvalidOperationException);
    }
    // check data remains
    AssertSchema.assertHasTuples(conn, KV_SCHEMA_TABLE, kvSchema, current1, current2);
    AssertSchema.assertHasTuples(conn, ROW_SCHEMA_TABLE, rowSchema, current1, current2);

    final PredicativeExpression deleteConditions = new EqualOperator(
        new AttributeNameExpression(KEY2, StringAttributeType.INSTANCE),
        new ConstantExpression(current2.getAttribute(KEY2))
    );
    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      valorConn.delete(relationId, deleteConditions);
    }
    // check data remains
    AssertSchema.assertHasTuples(conn, KV_SCHEMA_TABLE, kvSchema, current1);
    AssertSchema.assertHasTuples(conn, ROW_SCHEMA_TABLE, rowSchema, current1);
  }

  @Test
  public void testSchemaMode() throws Exception {
    String relationId = relation.getRelationId();

    repository.setSchemaMode(RELATION_ID, KV_SCHEMA_ID, Schema.Mode.READ_ONLY);
    repository.setSchemaMode(RELATION_ID, ROW_SCHEMA_ID, Schema.Mode.WRITE_ONLY);
    Tuple t1 = buildTuple("testMode_k1", "testMode_k2", "testMode_a2", "testMode_other");
    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      valorConn.insert(relationId, t1);
    }
    AssertSchema.assertNotHaveTuple(conn, KV_SCHEMA_TABLE, kvSchema, t1);
    AssertSchema.assertHasTuple(conn, ROW_SCHEMA_TABLE, rowSchema, t1);

    PredicativeExpression conditions = AndOperator.join(
        new EqualOperator(new AttributeNameExpression(KEY1, StringAttributeType.INSTANCE),
            new ConstantExpression(t1.getAttribute(KEY1))),
        new RegexpOperator(new AttributeNameExpression(ATTR1, StringAttributeType.INSTANCE),
            new ConstantExpression(".*")),
        new RegexpOperator(new AttributeNameExpression(ATTR2,
            StringAttributeType.INSTANCE), new ConstantExpression(".*"))
    );

    List<Tuple> selectResult;
    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      selectResult = valorConn.select(relationId, relation.getAttributeNames(), conditions);
    }
    assertEquals(0, selectResult.size());

    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      valorConn.select(ROW_SCHEMA_ID, relation.getAttributeNames(), conditions);
      fail();
    } catch (Exception e) {
      // ROW_SCHEMA_ID is write_only
    }

    repository.setSchemaMode(RELATION_ID, KV_SCHEMA_ID, Schema.Mode.PUBLIC);
    repository.setSchemaMode(RELATION_ID, ROW_SCHEMA_ID, Schema.Mode.PUBLIC);

    Tuple t2 = t1.getCopy();
    t2.setAttribute(ATTR1, "testPut_a_updated");
    t2.setAttribute(ATTR2, "testPut_a_updated");
    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      valorConn.insert(relationId, t2);
    }

    AssertSchema.assertHasTuple(conn, KV_SCHEMA_TABLE, kvSchema, t2);
    AssertSchema.assertHasTuple(conn, ROW_SCHEMA_TABLE, rowSchema, t2);
    System.out.println("dump");
    AssertSchema.dump(conn, KV_SCHEMA_TABLE);
    AssertSchema.dump(conn, ROW_SCHEMA_TABLE);

    try (ValorConnection valorConn = ValorConnectionFactory.create(context)) {
      selectResult = valorConn.select(relationId, relation.getAttributeNames(), conditions);
    }
    assertEquals(1, selectResult.size());
    AssertSchema.assertScanResultEquals(t2, selectResult.get(0));
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
