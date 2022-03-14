package jp.co.cyberagent.valor.sdk.storage.jdbc;

import java.util.Arrays;
import jp.co.cyberagent.valor.spi.Plugin;
import jp.co.cyberagent.valor.spi.storage.StorageFactory;

public class JdbcPlugin implements Plugin {
  public Iterable<StorageFactory> getStorageFactories() {
    return Arrays.asList(new JdbcStorage.Factory());
  }
}
