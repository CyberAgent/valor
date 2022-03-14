package jp.co.cyberagent.valor.sdk.storage;

import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.storage.Storage;

public abstract class StorageBase extends Storage {

  protected ValorConf conf;

  public StorageBase(ValorConf conf) {
    this.conf = conf;
  }

  @Override
  public ValorConf getConf() {
    return conf;
  }
}
