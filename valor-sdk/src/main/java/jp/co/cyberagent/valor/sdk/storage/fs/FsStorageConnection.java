package jp.co.cyberagent.valor.sdk.storage.fs;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import jp.co.cyberagent.valor.sdk.storage.kv.KeyValueStorageConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;

/**
 *
 */
public class FsStorageConnection implements KeyValueStorageConnection {
  @Override
  public void insert(Collection<Record> records) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(Collection<Record> records) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Long> count(SchemaScan scan) throws ValorException {
    throw new UnsupportedOperationException("count is not supported in " + getClass().getName());
  }

  @Override
  public StorageScanner getStorageScanner(StorageScan scan) throws ValorException {
    return new FsScanner(scan);
  }

  @Override
  public boolean isAvailable() {
    return true;
  }

  @Override
  public void close() throws IOException {
  }
}
