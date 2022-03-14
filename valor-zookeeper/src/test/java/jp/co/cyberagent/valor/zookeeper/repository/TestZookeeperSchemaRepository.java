package jp.co.cyberagent.valor.zookeeper.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.metadata.SchemaRepositoryTestBase;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.PortAssignment;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestZookeeperSchemaRepository extends SchemaRepositoryTestBase {

  public static final RetryNTimes RETRY_POLICY = new RetryNTimes(1, 100);
  private static ZookeeperSchemaRepository repository;
  private static ZookeeperSchemaRepository otherRepository;
  private static TestingServer zk;
  private static String rootNode;
  private static int port = PortAssignment.unique();

  private static ValorContext context;

  @BeforeAll
  public static void init() throws Exception {
    zk = new TestingServer(port, true);
    Map<String, String> confBase = new HashMap<>();
    confBase.put(StandardContextFactory.SCHEMA_REPOSITORY_CLASS.name,
        ZookeeperSchemaRepository.class.getCanonicalName());
    confBase.put(ZookeeperSchemaRepository.SCHEMA_REPOS_ZKQUORUM.name, zk.getConnectString());
    ValorConf conf = new ValorConfImpl(new HashMap<>(confBase));

    context = StandardContextFactory.create(conf);
    rootNode = ZookeeperSchemaRepository.SCHEMA_REPOS_ROOT_ZNODE.defaultValue;
    repository = new ZookeeperSchemaRepository(conf);
    repository.init(context);

    ValorConf otherConf = new ValorConfImpl(confBase);
    otherConf.set(ZookeeperSchemaRepository.SCHEMA_REPOS_ZK_TIMEOUT.name, "1000");
    ValorContext otherContext = StandardContextFactory.create(otherConf);
    otherRepository = new ZookeeperSchemaRepository(otherConf);
    otherRepository.init(otherContext);
    SchemaRepositoryTestBase.setInitialSchema(repository);
  }

  @AfterAll
  public static void tearDown() throws Exception {
    repository.close();
    otherRepository.close();
    zk.stop();
  }

  @BeforeEach
  public void initRepo() {
    this.repo = repository;
  }

  @Test
  public void testOtherRepository() throws Exception {

    Relation tmpRelation = ImmutableRelation.builder().relationId("temp_t").addAttribute("attr",
        true, StringAttributeType.INSTANCE).build();

    SchemaDescriptor tempSchemaDef =
        ImmutableSchemaDescriptor.builder().schemaId("temp_s").isPrimary(true)
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(new ValorConfImpl())
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(ConstantFormatter.create("temp_s")))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(AttributeValueFormatter.create("attr")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList())
            .build();

    repository.createRelation(tmpRelation, true);
    Thread.sleep(1000);
    assertTrue(repository.listRelationIds().contains("temp_t"));
    assertTrue(otherRepository.listRelationIds().contains("temp_t"));
    repository.createSchema("temp_t", tempSchemaDef, true);
    Thread.sleep(1000);
    assertTrue(repository.listSchemas("temp_t").stream().anyMatch(new SchemaRepository.IdMatcher(
        "temp_s")));
    assertTrue(otherRepository.listSchemas("temp_t").stream()
        .anyMatch(new SchemaRepository.IdMatcher("temp_s")));
    repository.dropSchema("temp_t", "temp_s");
    Thread.sleep(1000);
    assertFalse(repository.listSchemas("temp_t").stream()
        .anyMatch(new SchemaRepository.IdMatcher("temp_s")));
    assertFalse(otherRepository.listSchemas("temp_t").stream()
        .anyMatch(new SchemaRepository.IdMatcher("temp_s")));
    repository.dropRelation("temp_t");
    Thread.sleep(1000);
    assertFalse(repository.listRelationIds().contains("temp_t"));
    assertFalse(otherRepository.listRelationIds().contains("temp_t"));
  }

  @Test
  public void testChangeMode() throws Exception {
    String tupleId = SchemaRepositoryTestBase.relation.getRelationId();
    String schemaId = SchemaRepositoryTestBase.schemaDef.getSchemaId();
    repository.setSchemaMode(tupleId, schemaId, Schema.Mode.READ_ONLY);
    Thread.sleep(1000);
    assertEquals(Schema.Mode.READ_ONLY, repository.findSchema(tupleId, schemaId).getMode());
    assertEquals(Schema.Mode.READ_ONLY, otherRepository.findSchema(tupleId, schemaId).getMode());

    otherRepository.setSchemaMode(tupleId, schemaId, Schema.Mode.WRITE_ONLY);
    Thread.sleep(1000);
    assertEquals(Schema.Mode.WRITE_ONLY, repository.findSchema(tupleId, schemaId).getMode());
    assertEquals(Schema.Mode.WRITE_ONLY, otherRepository.findSchema(tupleId, schemaId).getMode());

    repository.setSchemaMode(tupleId, schemaId, Schema.Mode.PUBLIC);
    Thread.sleep(1000);
    assertEquals(Schema.Mode.PUBLIC, repository.findSchema(tupleId, schemaId).getMode());
    assertEquals(Schema.Mode.PUBLIC, otherRepository.findSchema(tupleId, schemaId).getMode());
  }

  @Override
  protected void assertExistenceInPersistentStore(Boolean shouldExist, String relId)
      throws Exception {
    try (CuratorFramework client = CuratorFrameworkFactory.newClient(zk.getConnectString(),
        RETRY_POLICY)) {
      client.start();
      Stat exists = client.checkExists().forPath(ZKPaths.makePath(rootNode, relId));
      assertEquals(shouldExist, exists != null);
    }
  }

  @Override
  protected void assertExisitenceInPersistentStore(Boolean shouldExist, String relId,
                                                   String schemaId) throws Exception {
    try (CuratorFramework client = CuratorFrameworkFactory.newClient(zk.getConnectString(),
        RETRY_POLICY)) {
      client.start();
      Stat exists = client.checkExists().forPath(ZookeeperSchemaRepository.joinPath(rootNode,
          relId, schemaId));
      assertEquals(shouldExist, exists != null);
    }
  }
}
