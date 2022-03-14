package jp.co.cyberagent.valor.sdk.metadata;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.IllegalRelationException;
import jp.co.cyberagent.valor.spi.exception.IllegalSchemaException;
import jp.co.cyberagent.valor.spi.exception.NamespaceMismatchException;
import jp.co.cyberagent.valor.spi.exception.UnknownRelationException;
import jp.co.cyberagent.valor.spi.exception.UnknownSchemaException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.MetadataSerde;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SchemaRepositoryBase implements SchemaRepository {

  public static String SCHEMA_REPOSITORY_CACHE_TTL = "valor.cache.ttlsec";

  static final Logger LOG = LoggerFactory.getLogger(SchemaRepositoryBase.class);

  private AtomicReference<CacheEntry<Collection<String>>> relationIds;

  private ConcurrentMap<String, CacheEntry<Relation>> relationCache;

  private ConcurrentMap<String, CacheEntry<Collection<Schema>>> schemaCache;

  protected ValorContext context;

  protected String defaultNamespace = DEFAULT_NAMESPACE;

  // TODO make configurable
  protected MetadataSerde serde;

  protected long cacheTtlMs = -1L;

  private boolean enableCache = false;

  protected SchemaRepositoryBase(ValorConf conf) {
    enableCache = conf.containsKey(SCHEMA_REPOSITORY_CACHE_TTL);
    if (enableCache) {
      cacheTtlMs = Long.valueOf(conf.get(SCHEMA_REPOSITORY_CACHE_TTL)) * 1000;
      relationCache = new ConcurrentHashMap<>();
      schemaCache = new ConcurrentHashMap<>();
      relationIds = new AtomicReference();
    }
  }

  @Override
  public void init(ValorContext context) {
    this.context = context;
    this.serde = new MetadataJsonSerde(context);
  }

  @Override
  public String getDefaultNameSpace() {
    return defaultNamespace;
  }

  @Override
  public Collection<String> getNamespaces() {
    return Arrays.asList(defaultNamespace);
  }

  @Override
  public Collection<String> listRelationIds(String namespace) throws ValorException {
    checkNamespace(namespace);
    if (!enableCache) {
      return doGetRelationIds();
    }

    CacheEntry<Collection<String>> relIdsCache = relationIds.get();
    if (relIdsCache != null && relIdsCache.isValid()) {
      return relIdsCache.getValue();
    }

    Collection<String> relIds = doGetRelationIds();
    relationIds.set(new CacheEntry<>(relIds, cacheTtlMs));
    return relIds;
  }

  protected abstract Collection<String> doGetRelationIds() throws ValorException;

  @Override
  public Relation findRelation(String namespace, String relId) throws ValorException {
    checkNamespace(namespace);
    if (!enableCache) {
      return doGetRelation(relId);
    }
    CacheEntry<Relation> relEntry = relationCache.get(relId);
    if (relEntry != null) {
      if (relEntry.isValid()) {
        return relEntry.getValue();
      }
      relationCache.remove(relId, relEntry);
    }

    Relation rel = doGetRelation(relId);
    if (rel != null) {
      relEntry = new CacheEntry<>(rel, cacheTtlMs);
      relationCache.putIfAbsent(relId, relEntry);
    }
    return rel;
  }

  protected abstract Relation doGetRelation(String relId) throws ValorException;

  @Override
  public synchronized Relation createRelation(
      String namespace, Relation relation, boolean overwrite) throws ValorException {
    checkNamespace(namespace);
    if (relation.getAttributeNames().isEmpty()) {
      new IllegalRelationException(relation.getRelationId(), "tuple does not have any attributes");
    }

    Relation prevEntry = findRelation(relation.getRelationId());
    if (prevEntry != null && !overwrite) {
      throw new IllegalRelationException(relation.getRelationId(), "relation is already defined");
    }

    doRegisterRelation(relation);
    if (enableCache) {
      CacheEntry<Relation> cacheEntry = new CacheEntry<>(relation, cacheTtlMs);
      relationCache.put(relation.getRelationId(), cacheEntry);
      relationIds.set(null);
    }
    LOG.info("tuple " + relation
        .getRelationId() + prevEntry == null ? " is registered" : " is overwritten");
    return prevEntry;
  }

  protected abstract void doRegisterRelation(Relation relation) throws ValorException;

  @Override
  public synchronized String dropRelation(String namespace, String relId) throws ValorException {
    checkNamespace(namespace);
    String removed = doDropRelation(relId);
    invalidateCache(relId);
    return removed;
  }

  protected abstract String doDropRelation(String relId) throws ValorException;

  @Override
  public Collection<Schema> listSchemas(String namespace, String relationId) throws ValorException {
    checkNamespace(namespace);
    if (!enableCache) {
      return doGetSchemas(relationId);
    }
    CacheEntry<Collection<Schema>> schemaEntries = schemaCache.get(relationId);
    if (schemaEntries != null) {
      if (schemaEntries.isValid()) {
        return schemaEntries.getValue();
      }
      schemaCache.remove(relationId, schemaEntries);
    }

    Collection<Schema> schemas = doGetSchemas(relationId);
    if (schemas == null) {
      schemas = Collections.emptyList();
    }
    if (enableCache) {
      schemaEntries = new CacheEntry<>(schemas, cacheTtlMs);
      schemaCache.putIfAbsent(relationId, schemaEntries);
    }
    return schemas;
  }

  protected abstract Collection<Schema> doGetSchemas(String relationId) throws ValorException;

  @Override
  public synchronized Schema findSchema(String namespace, String relationId, String schemaId)
      throws ValorException {
    checkNamespace(namespace);
    if (!enableCache) {
      return doGetSchema(relationId, schemaId);
    }
    CacheEntry<Collection<Schema>> schemaEntries = schemaCache.get(relationId);
    if (schemaEntries != null) {
      if (schemaEntries.isValid()) {
        Optional<Schema> optionalSchema = schemaEntries.getValue().stream()
            .filter(new IdMatcher(schemaId)).findFirst();
        return optionalSchema.orElse(null);
      }
      schemaCache.remove(relationId, schemaEntries);
    }

    Schema schema = doGetSchema(relationId, schemaId);
    if (schema == null) {
      return null;
    }
    Collection<Schema> schemas = doGetSchemas(relationId);
    if (schemas == null) {
      schemas = Collections.emptyList();
    }
    schemaCache.put(relationId, new CacheEntry<>(schemas, cacheTtlMs));
    return schemas.stream().filter(new IdMatcher(schemaId)).findFirst().orElse(null);
  }

  protected abstract Schema doGetSchema(String relationId, String schemaId)
      throws ValorException;

  @Override
  public synchronized Schema createSchema(
      String namespace, String relId, SchemaDescriptor schemaDescriptor, boolean overwrite)
      throws ValorException {
    checkNamespace(namespace);
    Relation relation = findRelation(relId);
    if (relation == null) {
      throw new UnknownRelationException(relId);
    }
    if (enableCache) {
      // disable cache for duplication checking
      schemaCache.remove(relId);
    }
    Collection<Schema> existingSchemas = doGetSchemas(relId);
    Schema prevEntry = null;
    IdMatcher idMatcher = new IdMatcher(schemaDescriptor.getSchemaId());
    if (existingSchemas == null) {
      existingSchemas = new HashSet<>();
    } else {
      prevEntry = existingSchemas.stream().filter(idMatcher).findFirst().orElse(null);
    }
    if (!overwrite && prevEntry != null) {
      throw new IllegalSchemaException(relId, schemaDescriptor.getSchemaId(),
          "schema is already defined");
    }

    Storage storage = context.createStorage(schemaDescriptor);
    Schema schema = storage.buildSchema(relation, schemaDescriptor);
    // check primary schema duplication
    existingSchemas = replaceOrAddSchema(relId, schema, existingSchemas);

    doCreateSchema(relId, schemaDescriptor);

    if (enableCache) {
      CacheEntry<Collection<Schema>> prevCacheEntry =
          schemaCache.putIfAbsent(relId, new CacheEntry<>(existingSchemas, cacheTtlMs));
      if (prevCacheEntry != null) {
        schemaCache.remove(relId);
      }
    }
    LOG.info("schema " + schemaDescriptor.getSchemaId() + prevEntry == null ? " is registered" :
        " is updated");
    return prevEntry;
  }

  protected abstract void doCreateSchema(String relId, SchemaDescriptor schema)
      throws ValorException;

  @Override
  public synchronized String dropSchema(String namespace, String relId, String schemaId)
      throws ValorException {
    checkNamespace(namespace);
    String removedId = doDropSchema(relId, schemaId);
    invalidateCache(relId, schemaId);
    return removedId;
  }

  protected abstract String doDropSchema(String relId, String schemaId) throws ValorException;

  protected void invalidateCache() {
    if (enableCache) {
      relationCache.clear();
      schemaCache.clear();
      relationIds.set(null);
    }
  }


  protected void invalidateCache(String relId) {
    if (enableCache) {
      relationCache.remove(relId);
      schemaCache.remove(relId);
      relationIds.set(null);
    }
  }

  protected void invalidateCache(String relId, String schemaId) {
    if (enableCache) {
      schemaCache.remove(relId);
    }
  }


  @Override
  public synchronized Schema.Mode setSchemaMode(
      String namespace, String relId, String schemaId, Schema.Mode mode) throws ValorException {
    checkNamespace(namespace);
    Relation entry = findRelation(relId);
    if (entry == null) {
      throw new UnknownRelationException(relId);
    }

    Collection<Schema> schemas = listSchemas(relId);
    if (schemas == null) {
      throw new UnknownSchemaException(relId, schemaId);
    }

    Schema schema = schemas.stream().filter(new IdMatcher(schemaId)).findAny().orElse(null);
    if (schema == null) {
      throw new UnknownSchemaException(relId, schemaId);
    }

    Schema.Mode prevMode = schema.getMode();
    schema.setMode(mode);
    SchemaDescriptor updated = SchemaDescriptor.from(schema);
    createSchema(relId, updated, true);
    return prevMode;
  }

  private void checkNamespace(String namespace) {
    if (namespace == null || namespace.equals(this.defaultNamespace)) {
      return;
    }
    throw new NamespaceMismatchException(this.defaultNamespace, namespace);
  }

  @Override
  public ValorContext getContext() {
    return context;
  }

  public static class CacheEntry<T> {

    private T value;
    private long validUntil;

    public CacheEntry(T value, long ttlMs) {
      this.value = value;
      this.validUntil = System.currentTimeMillis() + ttlMs;
    }

    public boolean isValid() {
      return validUntil > System.currentTimeMillis();
    }

    public T getValue() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CacheEntry)) {
        return false;
      }
      CacheEntry<?> that = (CacheEntry<?>) o;
      return validUntil == that.validUntil && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, validUntil);
    }

    @Override
    public String toString() {
      return String.format("<%d, %s>", validUntil, value);
    }
  }

}
