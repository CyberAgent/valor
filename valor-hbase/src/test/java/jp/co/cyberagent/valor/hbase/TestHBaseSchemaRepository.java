package jp.co.cyberagent.valor.hbase;

import static jp.co.cyberagent.valor.sdk.StandardContextFactory.SCHEMA_REPOSITORY_CLASS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import jp.co.cyberagent.valor.hbase.repository.HBaseSchemaRepository;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.metadata.SchemaRepositoryBase;
import jp.co.cyberagent.valor.sdk.metadata.SchemaRepositoryBase.CacheEntry;
import jp.co.cyberagent.valor.sdk.metadata.SchemaRepositoryTestBase;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestHBaseSchemaRepository extends SchemaRepositoryTestBase {

  private static SchemaRepository repository;

  private static HBaseTestingUtility utility;

  private static ValorContext context;

  private static Long cacheTtl = 1l;

  private static Connection hbaseConn;

  @BeforeAll
  public static void setup() throws Exception {
    utility = new HBaseTestingUtility();
    utility.cleanupTestDir();
    utility.startMiniCluster(1);
    utility.createTable(ByteUtils.toBytes(HBaseSchemaRepository.SCHEMA_TABLE.defaultValue),
        ByteUtils.toBytes(HBaseSchemaRepository.SCHEMA_FAMILY.defaultValue));
    Configuration clusterConf = utility.getConfiguration();

    String zkQuorumHost = clusterConf.get(HConstants.ZOOKEEPER_QUORUM);
    String zkQuorumPort = clusterConf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
    String zkQuorum = zkQuorumHost + ":" + zkQuorumPort;
    ValorConf conf = new ValorConfImpl();
    conf.set(SCHEMA_REPOSITORY_CLASS.name, HBaseSchemaRepository.NAME);
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    conf.set(SchemaRepositoryBase.SCHEMA_REPOSITORY_CACHE_TTL, String.valueOf(cacheTtl));

    context = StandardContextFactory.create(conf);
    context.installPlugin(new HBasePlugin());
    repository = context.createRepository(conf);
    SchemaRepositoryTestBase.setInitialSchema(repository);

    Configuration hbaseConf = new Configuration();
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum);
    hbaseConn = ConnectionFactory.createConnection(hbaseConf);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    repository.close();
    hbaseConn.close();
  }

  @BeforeEach
  public void initRepo() {
    this.repo = repository;
  }

  @Override
  protected void assertExistenceInPersistentStore(Boolean shouldExist, String relId)
      throws Exception {
    dump();
    final TableName tblName = TableName.valueOf(HBaseSchemaRepository.SCHEMA_TABLE.defaultValue);
    Get get = new Get(relId.getBytes());
    try (Table tbl = hbaseConn.getTable(tblName)) {
      boolean exist = tbl.exists(get);
      if (shouldExist) {
        assertThat("row key for " + relId + "should exist", exist);
      } else {
        assertThat("row key for " + relId + "should not exist", !exist);
      }
    }
  }

  @Override
  protected void assertExisitenceInPersistentStore(Boolean shouldExist, String relId,
      String schemaId) throws Exception {
    final TableName tblName = TableName.valueOf(HBaseSchemaRepository.SCHEMA_TABLE.defaultValue);
    final String family =HBaseSchemaRepository.SCHEMA_FAMILY.defaultValue;
    Get get = new Get(relId.getBytes());
    get.addColumn(family.getBytes(), schemaId.getBytes());
    try (Table tbl = hbaseConn.getTable(tblName)) {
      boolean exist = tbl.exists(get);
      if (shouldExist) {
        assertThat(String.format("cell for %s.%s should exist", relId, schemaId), exist);
      } else {
        assertThat(String.format("cell for %s.%s should not exist", relId, schemaId), !exist);
      }
    }
  }

  @Override
  protected void dump() throws Exception {
    final TableName tblName = TableName.valueOf(HBaseSchemaRepository.SCHEMA_TABLE.defaultValue);
    try (Table tbl = hbaseConn.getTable(tblName);
         ResultScanner scanner = tbl.getScanner(new Scan());
    ) {
      Result r;
      while ((r = scanner.next()) != null) {
        for (Cell c : r.listCells()) {
          System.out.println(c);
        }
      }
    }
  }



  @Test
  public void testSchemaRepositoryBaseCache() throws ValorException, NoSuchFieldException, IllegalAccessException, InterruptedException {
    repo.createRelation(relation, true);
    Field relationCache = SchemaRepositoryBase.class.getDeclaredField("relationCache");
    relationCache.setAccessible(true);
    Map<String, CacheEntry<Relation>> rel
        = (Map<String, CacheEntry<Relation>>) relationCache.get(repo);
    repo.createRelation(relation, true);
    assertTrue(rel.get(relation.getRelationId()).isValid());
    assertThat(rel.get(relation.getRelationId()).getValue(), equalTo(relation));
    Thread.sleep(cacheTtl * 1_000 + 10);
    // cache expires after the ttl
    assertFalse(rel.get(relation.getRelationId()).isValid());

    repo.createSchema("t", schemaDef, true);
    Field schemaCache = SchemaRepositoryBase.class.getDeclaredField("schemaCache");
    schemaCache.setAccessible(true);
    Map<String, CacheEntry<Collection<Schema>>> sch
        = (Map<String, CacheEntry<Collection<Schema>>>) schemaCache.get(repo);
    // load cache
    repo.listSchemas(relation.getRelationId());
    Storage storage = context.createStorage(schemaDef);
    Schema expected = storage.buildSchema(relation, schemaDef);
    Schema accutual = sch.get(relation.getRelationId()).getValue().stream().findFirst().get();
    assertThat(accutual.getSchemaId(), equalTo(expected.getSchemaId()));
    assertTrue(sch.get(relation.getRelationId()).isValid());
    Thread.sleep(cacheTtl * 1_000 + 10);
    // cache expires after the ttl
    assertFalse(sch.get(relation.getRelationId()).isValid());
  }
}
