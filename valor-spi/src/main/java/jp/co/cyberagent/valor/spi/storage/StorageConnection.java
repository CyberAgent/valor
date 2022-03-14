package jp.co.cyberagent.valor.spi.storage;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;

/**
 *
 */
public interface StorageConnection extends Closeable {

  default void insert(Record... record) throws ValorException {
    insert(Arrays.asList(record));
  }

  void insert(Collection<Record> records) throws ValorException;

  // TODO optimize multiple records delete
  default void delete(Record... record) throws ValorException {
    delete(Arrays.asList(record));
  }

  void delete(Collection<Record> records) throws ValorException;

  // experimental
  Optional<Long> count(SchemaScan scan) throws ValorException;

  StorageScanner getStorageScanner(StorageScan scan) throws IOException, ValorException;

  default List<StorageScan> split(StorageScan scan) throws ValorException {
    return Arrays.asList(scan);
  }

  boolean isAvailable() throws ValorException;
}
