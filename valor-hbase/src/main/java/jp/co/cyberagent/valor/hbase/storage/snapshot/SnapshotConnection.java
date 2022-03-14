package jp.co.cyberagent.valor.hbase.storage.snapshot;

import java.io.IOException;
import java.util.Collection;
import java.util.Optional;
import jp.co.cyberagent.valor.sdk.storage.kv.KeyValueStorageConnection;
import jp.co.cyberagent.valor.spi.exception.UnsupportedSchemaException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SnapshotConnection implements KeyValueStorageConnection {
  static final Logger LOG = LoggerFactory.getLogger(SnapshotConnection.class);
  private final Configuration conf;

  private String snapshot;

  public SnapshotConnection(String snapshot, Configuration conf) {
    this.snapshot = snapshot;
    this.conf = conf;
  }


  @Override
  public void insert(Collection<Record> records) throws ValorException {
    throw new UnsupportedSchemaException("hbase snapshot is read only");
  }

  @Override
  public void delete(Collection<Record> records) throws ValorException {
    throw new UnsupportedSchemaException("hbase snapshot is read only");
  }

  @Override
  public Optional<Long> count(SchemaScan scan) throws ValorException {
    return Optional.empty();
  }

  @Override
  public StorageScanner getStorageScanner(StorageScan scan) throws IOException, ValorException {
    // TODO push down scan
    return new SnapshotScanner(snapshot, conf);
  }

  @Override
  public boolean isAvailable() throws ValorException {
    return true;
  }

  @Override
  public void close() throws IOException {
  }

}
