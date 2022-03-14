package jp.co.cyberagent.valor.hbase;

import static jp.co.cyberagent.valor.sdk.StandardContextFactory.SCHEMA_REPOSITORY_CLASS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiFunction;
import jp.co.cyberagent.valor.hbase.repository.HBaseSchemaRepository;
import jp.co.cyberagent.valor.hbase.schema.HBaseDefaultSchemaHandler;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.hbase.util.AssertSchema;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;
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

public class TestHBaseCluster {

  private static final String HBASE_TABLE = "kvTable";
  private static final String HBASE_FAMILY = "kvFamily";

  private static HBaseTestingUtility utility;

  private static SchemaRepository repository;
  private static Connection conn;


  private static Storage storage;

  private static ValorContext context;
  private static ValorConfImpl conf;

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
    conf = new ValorConfImpl();
    conf.set(SCHEMA_REPOSITORY_CLASS.name, HBaseSchemaRepository.NAME);
    conf.set(ZookeeperSchemaRepository.SCHEMA_REPOS_ZKQUORUM.name, zkQuorum + ":" + zkQuorumPort);
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum + ":" + zkQuorumPort);

    context = StandardContextFactory.create(conf);
    context.installPlugin(new HBasePlugin());
    repository = context.createRepository(conf);

    utility.createTable(Bytes.toBytes(HBASE_TABLE), Bytes.toBytes(HBASE_FAMILY));
    conn = ConnectionFactory.createConnection(clusterConf);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    repository.close();
    conn.close();
    utility.shutdownMiniCluster();
  }

  @Test
  public void testKvSchema() throws Exception {
    final String relationId = "testKvSchema";
    final String schemaId = "kvSchema";
    final String keyAttr = "k1";
    final String valAttr = "v1";

    Relation relation = ImmutableRelation.builder()
        .relationId(relationId)
        .addAttribute(keyAttr, true, StringAttributeType.INSTANCE)
        .addAttribute(valAttr, false, IntegerAttributeType.INSTANCE)
        .build();
    SchemaDescriptor kvDescriptor =
        ImmutableSchemaDescriptor.builder().isPrimary(true)
            .schemaId(schemaId)
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(conf)
            .conf(conf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create(HBASE_TABLE)))
            .addField(HBaseCell.ROWKEY, Arrays.asList(
                VintSizePrefixHolder.create(ConstantFormatter.create(relationId)),
                AttributeValueFormatter.create(keyAttr)))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create(HBASE_FAMILY)))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(ConstantFormatter.create(relationId)))
            .addField(HBaseCell.VALUE, Arrays.asList(AttributeValueFormatter.create(valAttr)))
            .build();

    storage = context.createStorage(kvDescriptor);
    Schema schema = storage.buildSchema(relation, kvDescriptor);

    repository.createRelation(relation, true);
    repository.createSchema(relation.getRelationId(), kvDescriptor, true);

    BiFunction<String, Integer, Tuple> buildTuple = (k1, v1) -> {
      Tuple tuple = new TupleImpl(relation);
      tuple.setAttribute(keyAttr, k1);
      tuple.setAttribute(valAttr, v1);
      return tuple;
    };

    Tuple current1 = buildTuple.apply("valueOfk1", 100);
    Tuple current2 = buildTuple.apply("valueOfk2", 200);
    Tuple current3 = buildTuple.apply("valueOfk3", 300);
    Tuple current4 = buildTuple.apply("valueOfk4", 400);

    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.insert(relationId, current1);
      conn.insert(relationId, current2);
      conn.insert(relationId, current3);
      conn.insert(relationId, current4);
    }

    AssertSchema.assertHasTuples(conn, HBASE_TABLE, schema, current1, current2, current3, current4);

    PredicativeExpression conditions = OrOperator.join(
        new EqualOperator(keyAttr, StringAttributeType.INSTANCE, "valueOfk2"),
        new GreaterthanOperator(valAttr, IntegerAttributeType.INSTANCE, 200)
    );

    List<Tuple> selectResult;
    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      selectResult = conn.select(relationId, relation.getAttributeNames(), conditions);
    }
    assertEquals(3, selectResult.size());
  }


  @Test
  public void testDefaultSchema() throws Exception {
    final String relationId = "testDefaultSchema";
    final String keyAttr = "k1";
    final String valAttr = "v1";

    Relation relation = ImmutableRelation.builder()
        .relationId(relationId)
        .addAttribute(keyAttr, true, StringAttributeType.INSTANCE)
        .addAttribute(valAttr, false, IntegerAttributeType.INSTANCE)
        .schemaHandler(new HBaseDefaultSchemaHandler().configure(new HashMap() {
          {
            put(HBaseStorage.TABLE_PARAM_KEY, HBASE_TABLE);
            put(HBaseStorage.FAMILY_PARAM_KEY, HBASE_FAMILY);
            put(HConstants.ZOOKEEPER_QUORUM, conf.get(HConstants.ZOOKEEPER_QUORUM));
            put(HBaseDefaultSchemaHandler.ATTACH_RELID_AS_PREFIX_KEY, "true");
          }
        }))
        .build();

    repository.createRelation(relation, true);

    BiFunction<String, Integer, Tuple> buildTuple = (k1, v1) -> {
      Tuple tuple = new TupleImpl(relation);
      tuple.setAttribute(keyAttr, k1);
      tuple.setAttribute(valAttr, v1);
      return tuple;
    };

    Tuple current1 = buildTuple.apply("valueOfk1", 100);
    Tuple current2 = buildTuple.apply("valueOfk2", 200);
    Tuple current3 = buildTuple.apply("valueOfk3", 300);
    Tuple current4 = buildTuple.apply("valueOfk4", 400);

    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.insert(relationId, current1);
      conn.insert(relationId, current2);
      conn.insert(relationId, current3);
      conn.insert(relationId, current4);
    }

    PredicativeExpression conditions = OrOperator.join(
        new EqualOperator(keyAttr, StringAttributeType.INSTANCE, "valueOfk2"),
        new GreaterthanOperator(valAttr, IntegerAttributeType.INSTANCE, 200)
    );

    List<Tuple> selectResult;
    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      selectResult = conn.select(relationId, relation.getAttributeNames(), conditions);
    }
    assertEquals(3, selectResult.size());
  }


}
