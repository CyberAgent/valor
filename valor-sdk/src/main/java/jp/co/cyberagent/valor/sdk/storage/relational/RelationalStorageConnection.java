package jp.co.cyberagent.valor.sdk.storage.relational;

import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;

/**
 *
 */
public interface RelationalStorageConnection extends StorageConnection {

  void update(Record record);
}
