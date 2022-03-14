package jp.co.cyberagent.valor.sdk.metadata;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.IllegalRelationException;
import jp.co.cyberagent.valor.spi.exception.IllegalSchemaException;
import jp.co.cyberagent.valor.spi.exception.NamespaceMismatchException;
import jp.co.cyberagent.valor.spi.exception.UnknownRelationException;
import jp.co.cyberagent.valor.spi.exception.UnknownSchemaException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemorySchemaRepository implements SchemaRepository {

  public static final Logger LOG = LoggerFactory.getLogger(InMemorySchemaRepository.class);

  public static final String NAME = "memory";

  private String defaultNamespace = DEFAULT_NAMESPACE;
  protected Map<String, Relation> relations;
  protected Map<String, Collection<Schema>> schemaSets;
  protected ValorContext context;

  public InMemorySchemaRepository(ValorConf conf) {
    this.relations = new HashMap<>();
    this.schemaSets = new HashMap<>();
  }

  @Override
  public void init(ValorContext context) {
    this.context = context;
  }

  @Override
  public ValorContext getContext() {
    return context;
  }

  @Override
  public String getDefaultNameSpace() {
    return defaultNamespace;
  }

  @Override
  public Collection<String> listRelationIds(String namespace) {
    checkNamespace(namespace);
    return relations.keySet();
  }

  @Override
  public Relation findRelation(String namespace, String relId) {
    checkNamespace(namespace);
    return relations.get(relId);
  }

  @Override
  public synchronized Relation createRelation(
      String namespace, Relation relation, boolean overwrite) throws ValorException {
    checkNamespace(namespace);
    Relation prevEntry = relations.get(relation.getRelationId());
    if (!overwrite && prevEntry != null) {
      throw new IllegalRelationException(relation.getRelationId(), "relation is already defined");
    }
    relations.put(relation.getRelationId(), relation);
    return prevEntry;
  }

  @Override
  public synchronized String dropRelation(String namespace, String relId) {
    checkNamespace(namespace);
    Relation rel = relations.remove(relId);
    schemaSets.remove(relId);
    return rel == null ? null : relId;
  }

  @Override
  public Collection<Schema> listSchemas(String namespace, String relationId) {
    checkNamespace(namespace);
    return schemaSets.containsKey(relationId) ? schemaSets.get(relationId) : Collections.emptySet();
  }

  @Override
  public Schema findSchema(String namespace, String relationId, String schemaId) {
    checkNamespace(namespace);
    Collection<Schema> schemas = schemaSets.get(relationId);
    if (schemas == null) {
      return null;
    }
    return schemas.stream().filter(new IdMatcher(schemaId)).findFirst().orElse(null);
  }

  @Override
  public synchronized Schema createSchema(
      String namespace, String relId, SchemaDescriptor schemaDescriptor, boolean overwrite)
      throws ValorException {
    checkNamespace(namespace);
    Schema.checkSchemaId(relId, schemaDescriptor.getSchemaId());
    Relation relation = findRelation(relId);
    if (relation == null) {
      throw new UnknownRelationException(relId);
    }

    Collection<Schema> schemas = listSchemas(relId);
    Storage storage = context.createStorage(schemaDescriptor);
    Schema schema = storage.buildSchema(relation, schemaDescriptor);

    if (schemas == null) {
      schemas = new HashSet<>();
      schemas.add(schema);
      schemaSets.put(relId, schemas);
      return null;
    }

    IdMatcher idMatcher = new IdMatcher(schemaDescriptor.getSchemaId());
    Schema prevEntry = schemas.stream().filter(idMatcher).findFirst().orElse(null);
    if (!overwrite && prevEntry != null) {
      throw new IllegalSchemaException(relId, schemaDescriptor.getSchemaId(), "schema is already "
          + "defined");
    }
    schemas = schemas.stream().filter(idMatcher.negate()).collect(Collectors.toList());
    if (schemaDescriptor.isPrimary()) {
      Schema primary = schemas.stream().filter(Schema::isPrimary).findAny().orElse(null);
      if (primary != null) {
        throw new IllegalSchemaException(relId, schemaDescriptor.getSchemaId(), "primary schema "
            + "is already defined " + primary.getSchemaId());
      }
    }

    schemas.add(schema);
    schemaSets.put(relId, schemas);
    LOG.info("schema " + schemaDescriptor.getSchemaId() + prevEntry == null ? " is registered" :
        " is updated");
    return prevEntry;
  }

  @Override
  public synchronized String dropSchema(String namespace, String relId, String schemaId)
      throws ValorException {
    checkNamespace(namespace);
    Schema.checkSchemaId(relId, schemaId);
    if (findRelation(relId) == null) {
      throw new UnknownRelationException(relId);
    }
    Collection<Schema> schemas = schemaSets.get(relId);
    if (schemas == null) {
      throw new UnknownSchemaException(relId, schemaId);
    }
    Schema droppedSchema = null;
    Collection<Schema> updatedSchemas = new HashSet<>();
    for (Schema s : schemas) {
      if (Objects.equals(schemaId, s.getSchemaId())) {
        droppedSchema = s;
      } else {
        updatedSchemas.add(s);
      }
    }

    schemaSets.put(relId, updatedSchemas);
    return droppedSchema == null ? null : schemaId;
  }

  @Override
  public synchronized Schema.Mode setSchemaMode(
      String namespace, String relId, String schemaId, Schema.Mode mode)
      throws ValorException {
    checkNamespace(namespace);
    Relation entry = findRelation(relId);
    if (entry == null) {
      throw new UnknownRelationException(relId);
    }

    Collection<Schema> schemas = listSchemas(relId);
    if (schemas == null) {
      throw new UnknownSchemaException(relId, schemaId);
    }

    Schema targetSchema =
        schemas.stream().filter(new IdMatcher(schemaId)).findAny().orElse(null);
    if (targetSchema == null) {
      throw new UnknownSchemaException(relId, schemaId);
    }
    Schema.Mode prevMode = targetSchema.getMode();
    targetSchema.setMode(mode);
    return prevMode;
  }

  @Override
  public Collection<String> getNamespaces() {
    return Arrays.asList(defaultNamespace);
  }

  @Override
  public void close() {
    // nothing to do
  }

  private void checkNamespace(String namespace) {
    if (namespace == null || namespace.equals(this.defaultNamespace)) {
      return;
    }
    throw new NamespaceMismatchException(this.defaultNamespace, namespace);
  }

  public static class Factory implements SchemaRepositoryFactory {
    private ConcurrentMap<ValorConf, SchemaRepository> instances = new ConcurrentHashMap<>();

    @Override
    public SchemaRepository create(ValorConf conf) {
      SchemaRepository instance = instances.get(conf);
      if (instance != null) {
        return instance;
      }
      instance = new InMemorySchemaRepository(conf);
      SchemaRepository prevInstance = instances.putIfAbsent(conf, instance);
      return prevInstance == null ? instance : prevInstance;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends SchemaRepository> getProvidedClass() {
      return InMemorySchemaRepository.class;
    }
  }
}
