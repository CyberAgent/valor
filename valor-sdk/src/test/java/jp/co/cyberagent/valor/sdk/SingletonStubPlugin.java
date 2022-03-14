package jp.co.cyberagent.valor.sdk;

import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.Plugin;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageFactory;

// TOBE removed.
// This is a service loaded by serviceloader
// and src/test/resources/META-INF/services/jp.co.cyberagent.valor.spi.Plugin
// should be deleted together.
public class SingletonStubPlugin implements Plugin {

  public static final String NAME = "singletonStub";

  @Override
  public Iterable<SchemaRepositoryFactory> getSchemaRepositoryFactories() {
    return Arrays.asList(new SingletonStubRepositoryFactory());
  }

  @Override
  public Iterable<StorageFactory> getStorageFactories() {
    return Arrays.asList(new SingletonStubStorageFactory());
  }

  static class SingletonStubRepositoryFactory implements SchemaRepositoryFactory {

    static ConcurrentMap<ValorConf, SchemaRepository> repositories = new ConcurrentHashMap<>();

    @Override
    public SchemaRepository create(ValorConf config) {
      StubRepository rep = new StubRepository(config);
      SchemaRepository prevEntry = repositories.putIfAbsent(config, rep);
      if (prevEntry == null) {
        return rep;
      }
      rep.close();
      return prevEntry;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends SchemaRepository> getProvidedClass() {
      return StubRepository.class;
    }
  }

  static class SingletonStubStorageFactory extends StorageFactory {

    @Override
    protected Storage doCreate(ValorConf config) {
      return new EmbeddedEKVStorage(config);
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Storage> getProvidedClass() {
      return EmbeddedEKVStorage.class;
    }
  }

  // dummy class to avoid duplication of provided class by factory
  static class StubRepository extends InMemorySchemaRepository {

    public StubRepository(ValorConf conf) {
      super(conf);
    }
  }

}
