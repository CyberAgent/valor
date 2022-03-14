package jp.co.cyberagent.valor.spi.storage;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import jp.co.cyberagent.valor.spi.PluggableFactory;
import jp.co.cyberagent.valor.spi.ValorConf;

public abstract class StorageFactory implements PluggableFactory<Storage, ValorConf> {

  ConcurrentMap<ValorConf, Storage> storages = new ConcurrentHashMap();

  @Override
  public Storage create(ValorConf config) {
    Storage prevStorage = storages.get(config);
    if (prevStorage != null) {
      return prevStorage;
    }
    Storage newStorage = doCreate(config);
    prevStorage = storages.putIfAbsent(config, newStorage);
    return prevStorage == null ? newStorage : prevStorage;
  }

  protected abstract Storage doCreate(ValorConf conf);

}
