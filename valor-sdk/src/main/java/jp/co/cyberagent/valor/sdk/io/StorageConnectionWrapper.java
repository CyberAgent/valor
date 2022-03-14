package jp.co.cyberagent.valor.sdk.io;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;

public class StorageConnectionWrapper implements StorageConnection {

  protected final StorageConnection wrapped;

  public StorageConnectionWrapper(StorageConnection wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public void insert(Collection<Record> records) throws ValorException {
    wrapped.insert(records);
  }

  @Override
  public void delete(Collection<Record> records) throws ValorException {
    wrapped.delete(records);
  }

  @Override
  public Optional<Long> count(SchemaScan scan) throws ValorException {
    return wrapped.count(scan);
  }

  @Override
  public StorageScanner getStorageScanner(StorageScan scan) throws IOException, ValorException {
    return wrapped.getStorageScanner(scan);
  }

  @Override
  public List<StorageScan> split(StorageScan scan) throws ValorException {
    return wrapped.split(scan);
  }

  @Override
  public boolean isAvailable() throws ValorException {
    return wrapped.isAvailable();
  }

  @Override
  public void close() throws IOException {
    // nothing to do
  }

  public StorageConnection getWrapped() {
    return wrapped;
  }
}
