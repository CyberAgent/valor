package jp.co.cyberagent.valor.zookeeper.repository;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.metadata.SchemaRepositoryBase;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfParam;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.CuratorFrameworkFactory.Builder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperSchemaRepository extends SchemaRepositoryBase implements CuratorListener {

  public static final String ZNODE_PATH_SEPARATOR = "/";
  public static final ValorConfParam SCHEMA_REPOS_ZKQUORUM = new ValorConfParam(
      "valor.schemarepository.zookeeper.zkquorum", "localhost");
  public static final ValorConfParam SCHEMA_REPOS_ZK_TIMEOUT = new ValorConfParam(
      "valor.schemarepository.zookeeper.session.timeout", "90000");
  public static final ValorConfParam SCHEMA_REPOS_ROOT_ZNODE = new ValorConfParam(
      "valor.schemarepository.zookeeper.znode", "/valor");
  public static final ValorConfParam SCHEMA_REPOS_RETRY_COUNT = new ValorConfParam(
      "valor.schemarepository.zookeeper.retry.count", "3");
  public static final ValorConfParam SCHEMA_REPOS_RETRY_INTERVAL = new ValorConfParam(
      "valor.schemarepository.zookeeper.retry.interval", "1000");
  public static final String NAME = "zookeeper";
  static Logger LOG = LoggerFactory.getLogger(ZookeeperSchemaRepository.class);
  private CuratorFramework client;
  private String rootNode;

  public ZookeeperSchemaRepository(ValorConf conf) throws ValorException {
    super(conf);
    String zkQuorum = SCHEMA_REPOS_ZKQUORUM.get(conf);
    int sessionTimeout = Integer.parseInt(SCHEMA_REPOS_ZK_TIMEOUT.get(conf));
    int retryCount = Integer.parseInt(SCHEMA_REPOS_RETRY_COUNT.get(conf));
    int interval = Integer.parseInt(SCHEMA_REPOS_RETRY_INTERVAL.get(conf));
    Builder builder = CuratorFrameworkFactory.builder();
    builder.connectString(zkQuorum);
    builder.sessionTimeoutMs(sessionTimeout);
    builder.retryPolicy(new RetryNTimes(retryCount, interval));
    this.client = builder.build();
    this.client.getCuratorListenable().addListener(this);
    this.client.start();

    this.rootNode = SCHEMA_REPOS_ROOT_ZNODE.get(conf);
    if (!this.rootNode.startsWith(ZNODE_PATH_SEPARATOR) || this.rootNode
        .endsWith(ZNODE_PATH_SEPARATOR)) {
      throw new IllegalArgumentException(
          SCHEMA_REPOS_ROOT_ZNODE + " should start with " + ZNODE_PATH_SEPARATOR
              + " and not end with " + ZNODE_PATH_SEPARATOR);
    }
    try {
      if (client.checkExists().watched().forPath(this.rootNode) == null) {
        client.create().forPath(this.rootNode);
      }
    } catch (Exception e) {
      throw new ValorException(e);
    }
  }

  public static String joinPath(String... nodes) {
    if (!nodes[0].startsWith(ZNODE_PATH_SEPARATOR)) {
      throw new IllegalArgumentException("should start with the root node");
    }
    StringBuilder buf = new StringBuilder();
    buf.append(nodes[0]);
    for (int i = 1; i < nodes.length; i++) {
      buf.append(ZNODE_PATH_SEPARATOR).append(nodes[i]);
    }
    return buf.toString();
  }

  public static List<String> splitPath(String path) {
    return Arrays.stream(path.split(ZNODE_PATH_SEPARATOR)).filter(p -> !"".equals(p))
        .collect(Collectors.toList());
  }

  @Override
  protected Collection<String> doGetRelationIds() throws ValorException {
    try {
      return client.getChildren().watched().forPath(this.rootNode);
    } catch (Exception e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected Relation doGetRelation(String relId) {
    String path = ZKPaths.makePath(this.rootNode, relId);
    return getRelationFromZK(relId, path);
  }

  @Override
  protected void doRegisterRelation(Relation relation) throws ValorException {
    String path = ZKPaths.makePath(this.rootNode, relation.getRelationId());
    try {
      byte[] serializedRelation = serde.serialize(relation);
      if (client.checkExists().watched().forPath(path) == null) {
        client.create().forPath(path, serializedRelation);
      } else {
        client.setData().forPath(path, serializedRelation);
      }
      // set child watches
      client.getChildren().watched().forPath(path);
    } catch (Exception e) {
      throw new ValorException(e);
    }
  }

  @Override
  public String doDropRelation(String relId) throws ValorException {
    String path = ZKPaths.makePath(this.rootNode, relId);
    try {
      if (client.checkExists().watched().forPath(path) == null) {
        return null;
      } else {
        //Relation relation = findRelation(relId);
        for (String child : client.getChildren().watched().forPath(path)) {
          client.delete().forPath(ZKPaths.makePath(path, child));
        }
        client.delete().forPath(path);
        return relId;
      }
    } catch (Exception e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected Collection<Schema> doGetSchemas(String relId) throws ValorException {
    String path = ZKPaths.makePath(this.rootNode, relId);
    Relation relation = doGetRelation(relId);
    Collection<Schema> schemas = new HashSet<>();
    try {
      for (String schemaId : client.getChildren().watched().forPath(path)) {
        SchemaDescriptor descriptor
            = getSchemaFromZK(schemaId, joinPath(rootNode, relId, schemaId));
        Storage storage = getContext().createStorage(descriptor);
        schemas.add(storage.buildSchema(relation, descriptor));
      }
    } catch (Exception e) {
      throw new ValorException(e);
    }
    return schemas;
  }

  @Override
  protected Schema doGetSchema(String relId, String schemaId) throws ValorException {
    String path = joinPath(this.rootNode, relId, schemaId);
    Relation relation = doGetRelation(relId);
    SchemaDescriptor descriptor = getSchemaFromZK(schemaId, path);
    if (descriptor == null) {
      return null;
    }
    Storage storage = getContext().createStorage(descriptor);
    return storage.buildSchema(relation, descriptor);
  }

  @Override
  protected void doCreateSchema(String relId, SchemaDescriptor schema) throws ValorException {
    String path = joinPath(this.rootNode, relId, schema.getSchemaId());
    try {
      byte[] serializedSchema = serde.serialize(schema);
      if (client.checkExists().watched().forPath(path) == null) {
        client.create().forPath(path, serializedSchema);
      } else {
        client.setData().forPath(path, serializedSchema);
      }
    } catch (Exception e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected String doDropSchema(String relId, String schemaId) throws ValorException {
    String path = joinPath(this.rootNode, relId, schemaId);

    try {
      if (client.checkExists().watched().forPath(path) == null) {
        return null;
      } else {
        client.delete().forPath(path);
        return schemaId;
      }
    } catch (Exception e) {
      throw new ValorException(e);
    }
  }

  @Override
  public void eventReceived(CuratorFramework client, CuratorEvent event) {
    LOG.info("ZKEvent Received: " + event.toString());
    String path = event.getPath();
    if (path == null) {
      return;
    }
    // remove the root separator
    List<String> splittedPath = splitPath(event.getPath());

    EventType eventType = event.getWatchedEvent().getType();

    if (splittedPath.size() == 1) {
      if (EventType.NodeChildrenChanged.equals(eventType)) {
        // TODO don't update unchanged relations
        invalidateCache();
        LOG.info("Relations are updated");
      }
      return;
    } else if (splittedPath.size() == 2) {
      String relId = splittedPath.get(1);
      // tuple updates
      if (EventType.NodeCreated.equals(eventType) || EventType.NodeDataChanged.equals(eventType)) {
        LOG.info("Relation:" + relId + " is created/updated by other node");
        invalidateCache(relId);
      } else if (EventType.NodeDeleted.equals(eventType)) {
        LOG.info("Relation:" + relId + " is deleted by other node");
        invalidateCache(relId);
        // don't set watch on delete
        return;
      } else if (EventType.NodeChildrenChanged.equals(eventType)) {
        LOG.info("schema of " + relId + " is changed by other node");
        invalidateCache(relId);
      }
      // set watch
      try {
        client.getData().watched().forPath(path);
        client.getChildren().watched().forPath(path);
        return;
      } catch (Exception e) {
        LOG.warn("exception received while processing {}", event);
        throwIfNotClosed(e);
        return;
      }
    } else if (splittedPath.size() == 3) {
      String relId = splittedPath.get(1);
      // schema updates
      try {
        String schemaId = splittedPath.get(2);
        if (EventType.NodeCreated.equals(eventType) || EventType.NodeDataChanged
            .equals(eventType)) {
          LOG.info("schema {}.{} is create/updated by other node", relId, schemaId);
          invalidateCache(relId, schemaId);
        } else if (EventType.NodeDeleted.equals(eventType)) {
          LOG.info("schema {}.{} is removed by other node", relId, schemaId);
          invalidateCache(relId, schemaId);
        }
        return;
      } catch (Exception e) {
        LOG.warn("exception received while processing {}", event);
        throwIfNotClosed(e);
        return;
      }
    }
    throw new IllegalStateException("unsupported event " + event);
  }

  private Relation getRelationFromZK(String relId, String path) {
    byte[] def = getData(path);
    if (def == null) {
      return null;
    }
    try (InputStream is = new ByteArrayInputStream(def)) {
      return serde.readRelation(relId, is);
    } catch (Exception e) {
      LOG.warn("failed to parse a tuple definition from " + path, e);
      throwIfNotClosed(e);
      return null;
    }
  }

  private SchemaDescriptor getSchemaFromZK(String schemaId, String path) {
    byte[] def = getData(path);
    if (def == null) {
      return null;
    }
    try (InputStream is = new ByteArrayInputStream(def)) {
      return serde.readSchema(schemaId, is);
    } catch (Exception e) {
      LOG.warn("failed to parse a schema definition from " + path, e);
      throwIfNotClosed(e);
      return null;
    }
  }

  private byte[] getData(String path) {
    try {
      if (client.checkExists().watched().forPath(path) == null) {
        return null;
      }
      return client.getData().watched().forPath(path);
    } catch (Exception e) {
      throwIfNotClosed(e);
      return null;
    }
  }

  private void throwIfNotClosed(Exception exception) {
    if (exception instanceof IllegalStateException) {
      if (CuratorFrameworkState.STOPPED.equals(client.getState())) {
        LOG.warn("IllegalStateException {} received while closing this client",
            exception.getMessage());
        return;
      } else {
        throw (IllegalStateException) exception;
      }
    }
    throw new IllegalStateException("unexpected error", exception);
  }

  @Override
  public void close() throws IOException {
    LOG.info("closing curator connection");
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        throw new IOException(e);
      }
    }
  }

  public static class Factory implements SchemaRepositoryFactory {

    @Override
    public SchemaRepository create(ValorConf conf) {
      try {
        return new ZookeeperSchemaRepository(conf);
      } catch (ValorException e) {
        throw new IllegalArgumentException(e);
      }
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends SchemaRepository> getProvidedClass() {
      return ZookeeperSchemaRepository.class;
    }
  }
}
