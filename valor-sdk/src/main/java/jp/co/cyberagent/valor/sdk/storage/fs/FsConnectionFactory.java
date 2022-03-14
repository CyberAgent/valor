package jp.co.cyberagent.valor.sdk.storage.fs;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.sdk.storage.kv.KeyValueStorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;

public class FsConnectionFactory extends KeyValueStorageConnectionFactory {

  public static final List<String> FIELDS = Arrays.asList(FsCell.DIR, FsCell.FILE, FsCell.VALUE);

  public static final List<String> KEY_FIELDS = Arrays.asList(FsCell.DIR, FsCell.FILE);

  public FsConnectionFactory(FsStorage storage) {
    super(storage);
  }

  @Override
  public StorageConnection connect() {
    return new FsStorageConnection();
  }

  @Override
  public Storage getStorage() {
    return storage;
  }

  @Override
  public List<String> getRowkeyFields() {
    return KEY_FIELDS;
  }

  @Override
  public List<String> getKeyFields() {
    return KEY_FIELDS;
  }

  @Override
  public List<String> getFields() {
    return FIELDS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FsConnectionFactory that = (FsConnectionFactory) o;
    return Objects.equals(storage, that.storage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storage);
  }
}
