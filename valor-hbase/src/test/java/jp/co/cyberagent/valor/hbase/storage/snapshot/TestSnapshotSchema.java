package jp.co.cyberagent.valor.hbase.storage.snapshot;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import jp.co.cyberagent.valor.hbase.HBasePlugin;
import jp.co.cyberagent.valor.hbase.repository.HBaseSchemaRepository;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.zookeeper.repository.ZookeeperSchemaRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSnapshotSchema {


  private static final String RELATION_ID = "ssRel";
  private static final String SCHEMA_ID = "ssSchema";

  private static final byte[] TABLE = Bytes.toBytes("ssTable");
  private static final byte[] FAMILY = Bytes.toBytes("ssFamily");
  private static final byte[] QUALIFIER = Bytes.toBytes("");

  private static final String SNAPSHOT_NAME = "snapshot";

  private static byte[] SPLIT_KEY = new byte[]{0x00, 0x00, 0x00, 0x01};
  private static long REGION1_MASK = 0x0000000000000000L;
  private static long REGION2_MASK = 0x000000FF00000000L;
  private static long VALUE_MASK = 0x00000000FFFFFFFFL;

  private static final String KEY1 = "k1";
  private static final String VAL1 = "v1";

  private static HBaseTestingUtility utility;

  private static SchemaRepository repository;
  private static Connection conn;

  private static Relation relation;
  private static SchemaDescriptor schema;

  private static ValorContext context;


  @SuppressWarnings("resource")
  @BeforeAll
  public static void setup() throws Exception {

    utility = new HBaseTestingUtility();
    utility.cleanupTestDir();
    utility.startMiniCluster(2);
    utility.createTable(
        Bytes.toBytes(HBaseSchemaRepository.SCHEMA_TABLE.defaultValue),
        Bytes.toBytes(HBaseSchemaRepository.SCHEMA_FAMILY.defaultValue));
    utility.createTable(TABLE, FAMILY, new byte[][]{SPLIT_KEY});
    // insert sample data
    try (Connection conn = utility.getConnection();
         Table tbl = conn.getTable(TableName.valueOf(TABLE));
         Admin admin = conn.getAdmin()) {
      for (int i = 0; i < 24; i++) {
        if (i == 8) {
          admin.flush(tbl.getName());
          List<HRegionInfo> regions = admin.getTableRegions(tbl.getName());
          admin.splitRegion(regions.get(1).getRegionName());
        } else if (i == 16){
          admin.snapshot(SNAPSHOT_NAME, tbl.getName());
        }
        long key = i % 2 == 0 ? REGION1_MASK | (long)i : REGION2_MASK | (long)i;
        Put put = new Put(Bytes.toBytes(key));
        put.addColumn(FAMILY, QUALIFIER, Integer.toString(i).getBytes());
        tbl.put(put);
      }
      admin.flush(tbl.getName());
    }

    Configuration clusterConf = utility.getConfiguration();
    String zkQuorum = clusterConf.get(HConstants.ZOOKEEPER_QUORUM);
    String zkQuorumPort = clusterConf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
    ValorConf conf = new ValorConfImpl();
    conf.set(StandardContextFactory.SCHEMA_REPOSITORY_CLASS.name, HBaseSchemaRepository.NAME);
    conf.set(ZookeeperSchemaRepository.SCHEMA_REPOS_ZKQUORUM.name, zkQuorum + ":" + zkQuorumPort);
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum + ":" + zkQuorumPort);

    conf.set("hbase.rootdir", utility.getConfiguration().get("hbase.rootdir"));
    conf.set(HBaseSnapshotStorage.SNAPSHOT_NAME_KEY, SNAPSHOT_NAME);

    context = StandardContextFactory.create(conf);
    context.installPlugin(new HBasePlugin());
    repository = context.createRepository(conf);

    relation = ImmutableRelation.builder().relationId(RELATION_ID)
        .addAttribute(KEY1, true,  LongAttributeType.INSTANCE)
        .addAttribute(VAL1, false, StringAttributeType.INSTANCE)
        .build();
    schema =
        ImmutableSchemaDescriptor.builder().isPrimary(true)
            .schemaId(SCHEMA_ID)
            .storageClassName(HBaseSnapshotStorage.class.getCanonicalName())
            .storageConf(conf)
            .conf(conf)
            .addField(HBaseCell.ROWKEY, Arrays.asList(AttributeValueFormatter.create(KEY1)))
            .addField(HBaseCell.FAMILY,
                Arrays.asList(ConstantFormatter.create(Bytes.toString(FAMILY))))
            .addField(HBaseCell.QUALIFIER,
                Arrays.asList(ConstantFormatter.create(Bytes.toString(QUALIFIER))))
            .addField(HBaseCell.VALUE,
                Arrays.asList(AttributeValueFormatter.create(VAL1)))
            .build();
  }

  @Test
  public void testReadSnapshot() throws Exception {
    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.createRelation(relation, false);
      conn.createSchema(relation.getRelationId(), schema, false);

      RelationScan query = new RelationScan.Builder()
          .setRelationSource(null, relation)
          .build();
      testQuery(conn, query);
    }
  }

  private void testQuery(ValorConnection conn, RelationScan query) throws IOException, ValorException {
    Set<Long> expected = new HashSet<>(LongStream.range(0, 16)
        .mapToObj(Long.class::cast).collect(Collectors.toList()));
    List<Tuple> result = conn.scan(query);
    for (Tuple t : result) {
      long k = (long) t.getAttribute(KEY1);
      k = VALUE_MASK & k;
      String v = (String) t.getAttribute(VAL1);
      assertEquals(v, Long.toString(k));
      assertTrue(expected.remove(k));
    }
    assertTrue(expected.isEmpty());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    repository.close();
    utility.shutdownMiniCluster();
  }


}
