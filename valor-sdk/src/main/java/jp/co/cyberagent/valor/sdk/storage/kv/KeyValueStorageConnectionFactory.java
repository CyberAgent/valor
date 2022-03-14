package jp.co.cyberagent.valor.sdk.storage.kv;

import java.util.List;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;

public abstract class KeyValueStorageConnectionFactory extends StorageConnectionFactory {

  protected Storage storage;

  public KeyValueStorageConnectionFactory(Storage storage) {
    this.storage = storage;
  }

  public abstract List<String> getRowkeyFields();

  @Override
  public Storage getStorage() {
    return storage;
  }

}
