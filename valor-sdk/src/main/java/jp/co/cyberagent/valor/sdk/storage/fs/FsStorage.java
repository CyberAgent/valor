package jp.co.cyberagent.valor.sdk.storage.fs;

import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.sdk.storage.kv.KeyValueStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageFactory;

// TODO implement more proper base class
public class FsStorage extends KeyValueStorage {

  public static final String NAME = "file";

  public FsStorage(ValorConf conf) {
    super(conf);
  }

  @Override
  protected StorageConnectionFactory getConnectionFactory(Relation relation,
                                                          SchemaDescriptor descriptor) {
    return new FsConnectionFactory(this);
  }

  @Override
  protected List<String> getKeys() {
    return FsConnectionFactory.KEY_FIELDS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FsStorage other = (FsStorage) o;
    return Objects.equals(other.conf, conf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(conf);
  }


  public static class Factory extends StorageFactory {
    @Override
    protected Storage doCreate(ValorConf config) {
      return new FsStorage(config);
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Storage> getProvidedClass() {
      return FsStorage.class;
    }
  }
}
