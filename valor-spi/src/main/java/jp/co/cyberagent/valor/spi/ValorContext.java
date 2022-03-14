package jp.co.cyberagent.valor.spi;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Map;
import java.util.ServiceLoader;
import jp.co.cyberagent.valor.spi.conf.ValorConfParam;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.optimize.Optimizer;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.model.Udf;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaHandler;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Style(stagedBuilder = true)
@Value.Immutable
public interface ValorContext {

  Logger LOG = LoggerFactory.getLogger(ValorContext.class);

  String SCHEMA_REPOSITORY_CLASS_KEY = "valor.schemarepository.class";

  String PLANNER_CLASS_KEY = "valor.planner.class";

  ValorConfParam PLUGINS_DIR = new ValorConfParam("valor.plugins.dir", null);

  ValorConf getConf();

  PluggableManager<Storage, ValorConf> getStorageManager();

  PluggableManager<SchemaRepository, ValorConf> getSchemaRepositoryManager();

  PluggableManager<Formatter, Map> getFormatterManager();

  PluggableManager<Holder, Map> getHolderManager();

  PluggableManager<Planner, ValorConf> getPlannerManager();

  PluggableManager<Udf, Void> getUdfManager();

  PluggableManager<Optimizer, Map> getOptimizerManager();

  PluggableManager<SchemaHandler, Map> getSchemaHandlerManager();

  // delegation methods to factory classes

  default Storage createStorage(SchemaDescriptor descriptor) {
    return getStorageManager()
        .create(descriptor.getStorageClassName(), descriptor.getStorageConf());
  }

  default SchemaRepository createRepository(ValorConf conf) {
    String cls = conf.get(SCHEMA_REPOSITORY_CLASS_KEY);
    SchemaRepository repository = getSchemaRepositoryManager().create(cls, conf);
    repository.init(this);
    return repository;
  }

  default Segment createFormatter(String name, Map<String, Object> conf) {
    return getFormatterManager().create(name, conf);
  }

  default Holder createHolder(String name, Map<String, Object> conf) {
    return getHolderManager().create(name, conf);
  }

  default Planner createPlanner(String name, ValorConf conf) {
    return getPlannerManager().create(name, conf);
  }

  default Udf createUdf(String name) {
    return getUdfManager().create(name, null);
  }

  default Optimizer createOptimizer(String name, Map<String, Object> conf) {
    return getOptimizerManager().create(name, conf);
  }

  default SchemaHandler createSchemaHandler(String name, Map<String, Object> conf) {
    return getSchemaHandlerManager().create(name, conf);
  }

  // utility methods for simplicity

  /**
   * @deprecated use {@link #createRepository(ValorConf)}
   * @return
   */
  @Deprecated
  default SchemaRepository buildSchemaRepository() {
    return createRepository(getConf());
  }

  default Schema buildSchmea(Relation relaton, SchemaDescriptor schemaDescriptor)
      throws ValorException {
    Storage storage = createStorage(schemaDescriptor);
    return storage.buildSchema(relaton, schemaDescriptor);
  }

  default StorageConnection connect(Schema schema) throws ValorException {
    return schema.getConnectionFactory().connect();
  }

  /**
   *
   */
  default void loadPlugins() {
    // loaded default plugin
    ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class);
    LOG.info("loading default plugins");
    for (Plugin plugin : serviceLoader) {
      installPlugin(plugin);
    }
    String pathToPluginsDir = PLUGINS_DIR.get(getConf());
    if (pathToPluginsDir == null) {
      return;
    }

    LOG.info("loading plugins from " + pathToPluginsDir);
    File pluginsDir = new File(pathToPluginsDir);
    if (!pluginsDir.exists()) {
      return;
    }
    for (File plugin : pluginsDir.listFiles()) {
      LOG.info("loading plugin from {}", plugin.getAbsolutePath());
      if (!plugin.isDirectory()) {
        LOG.warn(plugin + " is not directory");
      }
      loadPlugin(plugin);
    }
  }

  default void loadPlugin(File pluginDir) {
    ArrayList<URL> cp = new ArrayList<>();
    try {
      cp.add(pluginDir.toURI().toURL());
      for (File jar : pluginDir.listFiles(f -> f.isFile() && f.getName().endsWith(".jar"))) {
        cp.add(jar.toURI().toURL());
      }
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
    URLClassLoader classLoader = new URLClassLoader(cp.toArray(new URL[cp.size()]),
        this.getClass().getClassLoader());
    try (ThreadContextClassLoader tccl = new ThreadContextClassLoader(classLoader)) {
      ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, classLoader);
      for (Plugin plugin : serviceLoader) {
        installPlugin(plugin);
      }
    }
  }

  default void installPlugin(Plugin plugin) {
    LOG.info("install plugin {}", plugin.getClass().getCanonicalName());
    getSchemaRepositoryManager().installPlugin(plugin);
    getStorageManager().installPlugin(plugin);
    getFormatterManager().installPlugin(plugin);
    getHolderManager().installPlugin(plugin);
    getOptimizerManager().installPlugin(plugin);
    getUdfManager().installPlugin(plugin);
    getSchemaHandlerManager().installPlugin(plugin);
  }

}
