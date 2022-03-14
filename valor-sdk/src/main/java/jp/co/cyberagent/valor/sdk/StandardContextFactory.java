package jp.co.cyberagent.valor.sdk;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.CurrentTimeFormatter;
import jp.co.cyberagent.valor.sdk.formatter.DateTime2LongFormatter;
import jp.co.cyberagent.valor.sdk.formatter.FilterMapFormatter;
import jp.co.cyberagent.valor.sdk.formatter.JqFormatter;
import jp.co.cyberagent.valor.sdk.formatter.JsonFormatter;
import jp.co.cyberagent.valor.sdk.formatter.Map2StringFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MapEntryFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MapKeyFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MapValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.Md5AttributeFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeNamesFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeValuesFormatter;
import jp.co.cyberagent.valor.sdk.formatter.Murmur3AttributeFormatter;
import jp.co.cyberagent.valor.sdk.formatter.Murmur3MapKeyFormatter;
import jp.co.cyberagent.valor.sdk.formatter.Murmur3SaltFormatter;
import jp.co.cyberagent.valor.sdk.formatter.NullableStringFormatter;
import jp.co.cyberagent.valor.sdk.formatter.Number2StringFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ReverseLongFormatter;
import jp.co.cyberagent.valor.sdk.formatter.Sha1AttributeFormatter;
import jp.co.cyberagent.valor.sdk.formatter.String2DateTimeFrameFormatter;
import jp.co.cyberagent.valor.sdk.formatter.String2NumberFormatter;
import jp.co.cyberagent.valor.sdk.formatter.UrlEncodeFormatter;
import jp.co.cyberagent.valor.sdk.holder.FixedLengthHolder;
import jp.co.cyberagent.valor.sdk.holder.OptionalHolder;
import jp.co.cyberagent.valor.sdk.holder.RegexpHolder;
import jp.co.cyberagent.valor.sdk.holder.SuffixHolder;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.metadata.CompositeSchemaRepository;
import jp.co.cyberagent.valor.sdk.metadata.FileSchemaRepository;
import jp.co.cyberagent.valor.sdk.metadata.HttpsSchemaRepository;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.plan.JoinablePlanner;
import jp.co.cyberagent.valor.sdk.plan.NaivePrimitivePlanner;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfArrayContains;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfArrayValue;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfMapKeys;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfMapValue;
import jp.co.cyberagent.valor.sdk.storage.fs.FsStorage;
import jp.co.cyberagent.valor.spi.ImmutableValorContext;
import jp.co.cyberagent.valor.spi.PluggableFactory;
import jp.co.cyberagent.valor.spi.PluggableManager;
import jp.co.cyberagent.valor.spi.Plugin;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.conf.ValorConfParam;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.spi.optimize.Optimizer;
import jp.co.cyberagent.valor.spi.optimize.OptimizerFactory;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.PlannerFactory;
import jp.co.cyberagent.valor.spi.plan.model.Udf;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.HolderFactory;
import jp.co.cyberagent.valor.spi.schema.SchemaHandler;
import jp.co.cyberagent.valor.spi.schema.SchemaHandlerFactory;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageFactory;

public class StandardContextFactory {

  public static final ValorConfParam SCHEMA_REPOSITORY_CLASS =
      new ValorConfParam(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, InMemorySchemaRepository.NAME);
  public static final ValorConfParam PLANNER_CLASS =
      new ValorConfParam(ValorContext.PLANNER_CLASS_KEY, NaivePrimitivePlanner.NAME);



  static ConcurrentMap<ValorConf, ValorContext> contexts = new ConcurrentHashMap<>();

  public static Function<ValorConf, ValorContext> defaultContextFactory = (conf) ->
      ImmutableValorContext.builder().conf(conf)
          .storageManager(new StorageManager())
          .schemaRepositoryManager(new SchemaRepositoryManager())
          .formatterManager(new RecordFormatterManager())
          .holderManager(new HolderManager())
          .plannerManager(new PlannerManager())
          .udfManager(new UdfManager())
          .optimizerManager(new OptimizerManager())
          .schemaHandlerManager(new SchemaHandlerManager())
          .build();

  public static ValorContext create() {
    ValorConf conf = new ValorConfImpl();
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, InMemorySchemaRepository.NAME);
    return create(conf);
  }

  public static ValorContext create(Map<String, String> conf) {
    return create(new ValorConfImpl(conf));
  }

  public static ValorContext create(ValorConf conf) {
    ValorContext prev = contexts.get(conf);
    if (prev != null) {
      return prev;
    }
    ValorContext context = defaultContextFactory.apply(conf);
    prev = contexts.putIfAbsent(conf, context);
    if (prev != null) {
      return prev;
    } else {
      context.loadPlugins();
      return context;
    }
  }

  static class StorageManager extends PluggableManager<Storage, ValorConf> {

    public StorageManager() {
      addFactory(new FsStorage.Factory());
    }

    @Override
    public void installPlugin(Plugin plugin) {
      for (StorageFactory factory : plugin.getStorageFactories()) {
        addFactory(factory);
      }
    }
  }

  static class SchemaRepositoryManager extends PluggableManager<SchemaRepository, ValorConf> {

    public SchemaRepositoryManager() {
      addFactory(new InMemorySchemaRepository.Factory());
      addFactory(new FileSchemaRepository.Factory());
      addFactory(new HttpsSchemaRepository.Factory());
      addFactory(new CompositeSchemaRepository.Factory());
    }

    @Override
    public void installPlugin(Plugin plugin) {
      for (SchemaRepositoryFactory factory : plugin.getSchemaRepositoryFactories()) {
        addFactory(factory);
      }
    }
  }

  static class RecordFormatterManager extends PluggableManager<Formatter, Map> {

    public RecordFormatterManager() {
      addFactory(new AttributeValueFormatter.Factory());
      addFactory(new ConstantFormatter.Factory());
      addFactory(new CurrentTimeFormatter.Factory());
      addFactory(new DateTime2LongFormatter.Factory());
      addFactory(new JqFormatter.Factory());
      addFactory(new JsonFormatter.Factory());
      addFactory(new FilterMapFormatter.Factory());
      addFactory(new Map2StringFormatter.Factory());
      addFactory(new MapEntryFormatter.Factory());
      addFactory(new MapKeyFormatter.Factory());
      addFactory(new MapValueFormatter.Factory());
      addFactory(new Md5AttributeFormatter.Factory());
      addFactory(new MultiAttributeNamesFormatter.Factory());
      addFactory(new MultiAttributeValuesFormatter.Factory());
      addFactory(new Murmur3AttributeFormatter.Factory());
      addFactory(new Murmur3MapKeyFormatter.Factory());
      addFactory(new Murmur3SaltFormatter.Factory());
      addFactory(new Number2StringFormatter.Factory());
      addFactory(new ReverseLongFormatter.Factory());
      addFactory(new Sha1AttributeFormatter.Factory());
      addFactory(new String2DateTimeFrameFormatter.Factory());
      addFactory(new String2NumberFormatter.Factory());
      addFactory(new UrlEncodeFormatter.Factory());
      addFactory(new NullableStringFormatter.Factory());
    }

    @Override
    public void installPlugin(Plugin plugin) {
      Iterable<FormatterFactory> formatterFactories = plugin.getFormatterFactories();
      for (FormatterFactory factory : formatterFactories) {
        addFactory(factory);
      }
    }
  }

  static class HolderManager extends PluggableManager<Holder, Map> {

    public HolderManager() {
      addFactory(new FixedLengthHolder.Factory());
      addFactory(new RegexpHolder.Factory());
      addFactory(new SuffixHolder.Factory());
      addFactory(new VintSizePrefixHolder.Factory());
      addFactory(new OptionalHolder.Factory());
    }

    @Override
    public void installPlugin(Plugin plugin) {
      for (HolderFactory factory : plugin.getHolderFactories()) {
        addFactory(factory);
      }
    }
  }

  static class PlannerManager extends PluggableManager<Planner, ValorConf> {

    public PlannerManager() {
      addFactory(new NaivePrimitivePlanner.Factory());
      addFactory(new JoinablePlanner.Factory());
    }

    @Override
    public void installPlugin(Plugin plugin) {
      for (PlannerFactory factory : plugin.getPlannerFactories()) {
        addFactory(factory);
      }
    }
  }

  static class UdfManager extends PluggableManager<Udf, Void> {

    public UdfManager() {
      addFactory(new UdfArrayContains.Factory());
      addFactory(new UdfArrayValue.Factory());
      addFactory(new UdfMapKeys.Factory());
      addFactory(new UdfMapValue.Factory());
    }

    @Override
    public void installPlugin(Plugin plugin) {
      for (PluggableFactory<Udf, Void> factory : plugin.getUdfFactories()) {
        addFactory(factory);
      }
    }
  }

  static class OptimizerManager extends PluggableManager<Optimizer, Map> {

    @Override
    public void installPlugin(Plugin plugin) {
      for (OptimizerFactory factory : plugin.getOptimizerFactories()) {
        addFactory(factory);
      }
    }
  }

  static class SchemaHandlerManager extends PluggableManager<SchemaHandler, Map> {

    @Override
    public void installPlugin(Plugin plugin) {
      for (SchemaHandlerFactory factory : plugin.getSchemaHandlerFactories()) {
        addFactory(factory);
      }
    }
  }
}
