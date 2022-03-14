package jp.co.cyberagent.valor.spi.storage;

import java.util.List;
import jp.co.cyberagent.valor.spi.exception.ValorException;

public abstract class StorageConnectionFactory {

  public abstract StorageConnection connect() throws ValorException;

  public abstract List<String> getKeyFields() throws ValorException;

  public abstract List<String> getFields() throws ValorException;

  public abstract Storage getStorage();

  @Override
  public abstract boolean equals(Object o);

  @Override
  public abstract int hashCode();

}
