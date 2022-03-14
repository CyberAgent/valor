package jp.co.cyberagent.valor.sdk.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.conf.ValorConfParam;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.util.Closer;

public class CompositeSchemaRepository implements SchemaRepository {

  public static final String NAME = "composite";

  public static final ValorConfParam CONF_DIR = new ValorConfParam(
      "valor.schemarepository.compsite.confdir", null);

  public static final ValorConfParam DEFAULT_NS = new ValorConfParam(
      "valor.schemarepository.compsite.defaultNs", "default");

  private final Map<String, SchemaRepository> repositories = new HashMap<>();
  private SchemaRepository defaultRepository;
  private ValorContext context;

  public CompositeSchemaRepository(ValorConf valorConf) {
    context = StandardContextFactory.create(valorConf);
    String defaultNs = DEFAULT_NS.get(valorConf);
    File confDir = new File(CONF_DIR.get(valorConf));
    ObjectMapper om = new ObjectMapper();
    for (File conf : confDir.listFiles()) {
      try {
        Map m = om.readValue(conf, Map.class);
        ValorConf repoConf = new ValorConfImpl((Map<String, String>) m);
        SchemaRepository repo = context.createRepository(repoConf);
        String ns = conf.getName();
        ns = ns.substring(0, ns.lastIndexOf("."));
        repositories.put(ns, repo);
        if (defaultNs.equals(ns)) {
          defaultRepository = repo;
        }
      } catch (IOException e) {
        throw new ValorRuntimeException("failed to parse conf " + conf.getAbsolutePath(), e);
      }
    }

  }

  public Collection<String> getNamespaces() {
    return repositories.keySet();
  }

  public SchemaRepository get(String namespace) {
    if (namespace == null) {
      return defaultRepository;
    }
    if (repositories.containsKey(namespace)) {
      return repositories.get(namespace);
    }
    throw new IllegalArgumentException(namespace + " is not a registered");
  }

  public void add(String name, ValorConf conf, ValorContext context) throws IOException {
    if (repositories.containsKey(name)) {
      throw new IllegalArgumentException(name + " is already registered");
    }
    SchemaRepository repo = context.createRepository(conf);
    SchemaRepository prev = repositories.putIfAbsent(name, repo);
    if (prev != null) {
      repo.close();
      throw new IllegalArgumentException(name + " is already registered");
    }
  }

  @Override
  public void close() throws IOException {
    Closer closer = new Closer();
    closer.close(defaultRepository);
    repositories.values().forEach(closer::close);
    closer.throwIfFailed();
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
    return DEFAULT_NAMESPACE;
  }

  @Override
  public Collection<String> listRelationIds(String namespace) throws ValorException {
    SchemaRepository repository = get(namespace);
    return repository.listRelationIds();
  }

  @Override
  public Relation findRelation(String namespace, String relationId) throws ValorException {
    SchemaRepository repository = get(namespace);
    return repository.findRelation(relationId);
  }

  @Override
  public Relation createRelation(String namespace, Relation relation, boolean overwrite)
      throws ValorException {
    SchemaRepository repository = get(namespace);
    return repository.createRelation(relation, overwrite);
  }

  @Override
  public String dropRelation(String namespace, String relationId) throws ValorException {
    SchemaRepository repository = get(namespace);
    return repository.dropRelation(namespace, relationId);
  }

  @Override
  public Collection<Schema> listSchemas(String namespace, String relationId) throws ValorException {
    SchemaRepository repository = get(namespace);
    return repository.listSchemas(relationId);
  }

  @Override
  public Schema findSchema(String namespace, String relationId, String schemaId)
      throws ValorException {
    SchemaRepository repository = get(namespace);
    return repository.findSchema(relationId, schemaId);
  }

  @Override
  public Schema createSchema(String namespace, String relationId, SchemaDescriptor schema,
                             boolean overwrite) throws ValorException {
    SchemaRepository repository = get(namespace);
    return repository.createSchema(relationId, schema, overwrite);
  }

  @Override
  public String dropSchema(String namespace, String relationId, String schemaId)
      throws ValorException {
    SchemaRepository repository = get(namespace);
    return repository.dropSchema(relationId, schemaId);
  }

  @Override
  public Schema.Mode setSchemaMode(String namespace, String relationId, String schemaId,
                                   Schema.Mode mode) throws ValorException {
    SchemaRepository repository = get(namespace);
    return repository.setSchemaMode(relationId, schemaId, mode);
  }

  public static class Factory implements SchemaRepositoryFactory {
    @Override
    public SchemaRepository create(ValorConf conf) {
      return new CompositeSchemaRepository(conf);
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends SchemaRepository> getProvidedClass() {
      return CompositeSchemaRepository.class;
    }
  }
}
