package jp.co.cyberagent.valor.sdk.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.DisabledOperationException;
import jp.co.cyberagent.valor.spi.exception.InvalidOperationException;
import jp.co.cyberagent.valor.spi.exception.UnknownSchemaException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.QueryPlan;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionClause;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.plan.model.Query;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.plan.model.RelationSource;
import jp.co.cyberagent.valor.spi.plan.model.WhereClause;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScanner;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;
import jp.co.cyberagent.valor.spi.util.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValorConnectionImpl implements ValorConnection {

  static Logger LOG = LoggerFactory.getLogger(ValorConnectionImpl.class);

  private final ValorContext context;

  private final SchemaRepository repository;

  // TODO make configurable
  private final Planner defaultPlanner;

  private final ConcurrentMap<StorageConnectionFactory, StorageConnection> connections
      = new ConcurrentHashMap<>();

  public ValorConnectionImpl(ValorContext context) {
    this.context = context;
    this.repository = context.createRepository(context.getConf());
    final String plannerName = StandardContextFactory.PLANNER_CLASS.get(context.getConf());
    this.defaultPlanner = context.createPlanner(plannerName, context.getConf());
  }

  @Override
  public SchemaRepository getSchemaRepository() {
    return repository;
  }

  @Override
  public Collection<String> getNamespaces() {
    return repository.getNamespaces();
  }

  @Override
  public ValorContext getContext() {
    return context;
  }

  @Override
  public StorageConnection createConnection(Schema schema) throws ValorException {
    StorageConnection conn = getConnection(schema);
    return new StorageConnectionWrapper(conn);
  }

  @Override
  public List<Tuple> select(String namespace, String relationId,
                            List<String> selectItems, PredicativeExpression condition)
      throws ValorException, IOException {
    Query query = toQuery(null, relationId, selectItems, condition);
    return select(query);
  }

  @Override
  public List<Tuple> scan(RelationScan scan) throws ValorException {
    List<Tuple> result = new ArrayList<>();
    int cnt = scan.getLimit();
    try (TupleScanner scanner = compile(scan)) {
      Tuple tuple;
      while ((tuple = scanner.next()) != null && cnt != 0) {
        result.add(tuple);
        cnt--;
      }
    } catch (IOException e) {
      throw new ValorException(e);
    }
    return result;
  }

  @Override
  public QueryPlan plan(Query query) throws ValorException {
    ScanPlan plan = defaultPlanner.plan(query, repository);
    if (plan == null) {
      throw new InvalidOperationException("failed to build plan for " + query);
    }
    return (QueryPlan) plan;
  }

  @Override
  public TupleScanner compile(RelationScan scan) throws ValorException {
    RelationSource source = scan.getFrom();
    Relation relation = source.getRelation();
    Collection<Schema> schemas = listSchemas(source.getNamespace(), relation.getRelationId());
    return relation.scanner(this, schemas, scan.getItems(), scan.getCondition(), defaultPlanner);
  }

  @Override
  public void insert(String namespacee, String relationId, Collection<Tuple> tuples)
      throws IOException, ValorException {
    Collection<Schema> candidateSchemas
        = getCandidateSchemas(namespacee, relationId, relationId, Relation.OperationType.WRITE);
    Relation relation = findRelation(namespacee, relationId);
    relation.insert(this, candidateSchemas, tuples);
  }

  @Override
  public int delete(String namespace, String relationId,
                    PredicativeExpression condition, int limit)
      throws IOException, ValorException {
    Collection<Schema> targetSchema
        = getCandidateSchemas(namespace, relationId, relationId, Relation.OperationType.WRITE);
    Relation relation = findRelation(namespace, relationId);
    List<ProjectionItem> items = relation.getAttributes().stream()
        .map(a -> new AttributeNameExpression(a.name(), a.type()))
        .map(a -> new ProjectionItem(a, a.getName()))
        .collect(Collectors.toList());
    // TODO use only keys for projection items
    try (TupleScanner scanner = relation.scanner(
        this, targetSchema, items, condition, defaultPlanner)) {
      return relation.delete(this, targetSchema, scanner, limit);
    }
  }

  @Override
  public int update(String namespace, String relationId,
                    Map<String, Object> newVals, PredicativeExpression condition)
      throws IOException, ValorException {
    Relation relation = findRelation(namespace, relationId);
    if (newVals.keySet().stream().anyMatch(a -> relation.getKeyAttributeNames().contains(a))) {
      throw new InvalidOperationException("key attributes cannot be updated");
    }
    Collection<Schema> targetSchema
        = getCandidateSchemas(namespace, relationId, relationId, Relation.OperationType.WRITE);
    List<ProjectionItem> items = relation.getAttributes().stream()
        .map(a -> new AttributeNameExpression(a.name(), a.type()))
        .map(a -> new ProjectionItem(a, a.getName()))
        .collect(Collectors.toList());
    try (TupleScanner scanner = relation.scanner(
        this, targetSchema, items, condition, defaultPlanner)) {
      return relation.update(this, targetSchema, scanner, newVals);
    }
  }

  @Override
  public boolean isRelationAvailable(String repoName, String relationId)
      throws ValorException, IOException {
    Optional<Schema> schema = repository.listSchemas(repoName, relationId).stream().findFirst();
    if (!schema.isPresent()) {
      return false;
    }
    try (StorageConnection conn = getConnection(schema.get())) {
      return conn.isAvailable();
    }
  }

  @Override
  public void close() throws IOException {
    Closer closer = new Closer();
    connections.values().forEach(closer::close);
    closer.close(repository);
    closer.throwIfFailed();
  }

  private Query toQuery(String namespace, String relationId,
                        List<String> attributes, PredicativeExpression condition)
      throws ValorException {
    Relation relation = findRelation(namespace, relationId);
    RelationSource source = new RelationSource(namespace, relation);
    List<Expression> items = new ArrayList<>(attributes.size());
    for (String attr : attributes) {
      AttributeType type = relation.getAttribute(attr).type();
      items.add(new AttributeNameExpression(attr, type));
    }
    return new Query(new ProjectionClause(items), source, new WhereClause(condition));
  }

  private Collection<Schema> getCandidateSchemas(
      String namespace, String relationId, String schemaId, Relation.OperationType opType)
      throws ValorException {
    if (isRelationId(relationId, schemaId)) {
      Collection<Schema> schemas = repository.listSchemas(namespace, relationId);
      List<Schema> targetSchemas = new ArrayList<>(schemas.size());
      for (Schema schema : schemas) {
        if (schema.getMode().isAllowed(opType)) {
          targetSchemas.add(schema);
        }
      }
      return targetSchemas;
    }

    Schema schema = repository.findSchema(relationId, schemaId);
    if (schema == null) {
      throw new UnknownSchemaException(relationId, schemaId);
    }
    if (!schema.getMode().isAllowed(opType)) {
      throw new DisabledOperationException(relationId, schemaId, opType);
    }
    return Arrays.asList(schema);
  }

  @Deprecated
  public SchemaScanner buildScanner(SchemaScan scan) throws ValorException, IOException {
    StorageConnection conn = getConnection(scan.getSchema());
    return scan.getSchema().getScanner(scan, conn);
  }

  private boolean isRelationId(String relationId, String schemaId) {
    return schemaId == null || schemaId.equals(relationId);
  }

  @Override
  public void submit(Schema schmea, StorageMutation mutation) throws ValorException, IOException {
    StorageConnection sc = getConnection(schmea);
    mutation.execute(sc);
  }

  private StorageConnection getConnection(Schema schema) throws ValorException {
    // TODO consider thread unsafe connection
    StorageConnectionFactory factory = schema.getConnectionFactory();
    StorageConnection conn = connections.get(factory);
    if (conn != null) {
      return conn;
    }
    conn = factory.connect();
    StorageConnection prevConn = connections.putIfAbsent(factory, conn);
    if (prevConn == null) {
      return conn;
    }
    try {
      conn.close();
    } catch (IOException e) {
      throw new ValorException("failed to close duplicated connection", e);
    }
    return prevConn;
  }

}
