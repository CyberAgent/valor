package jp.co.cyberagent.valor.sdk.storage.jdbc;

import static jp.co.cyberagent.valor.sdk.storage.jdbc.JdbcStorage.JDBC_URL;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.storage.relational.RelationalStorage;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;

public class JdbcConnectionFactory extends StorageConnectionFactory {

  private final RelationalStorage storage;
  private String tableName;
  private List<JdbcStorage.ColumnDesc> columns;

  public JdbcConnectionFactory(RelationalStorage storage, String tableName,
                               List<JdbcStorage.ColumnDesc> columns) {
    this.storage = storage;
    this.tableName = tableName;
    this.columns = columns;
  }

  @Override
  public StorageConnection connect() throws ValorException {
    return new JdbcStorageConnection(tableName, columns, JDBC_URL.get(storage.getConf()));
  }

  @Override
  public RelationalStorage getStorage() {
    return null;
  }

  @Override
  public List<String> getKeyFields() {
    if (columns == null) {
      return null;
    }
    return columns.stream().filter(c -> c.key).map(c -> c.name).collect(Collectors.toList());
  }

  @Override
  public List<String> getFields() {
    if (columns == null) {
      return null;
    }
    return columns.stream().map(c -> c.name).collect(Collectors.toList());
  }



  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    JdbcConnectionFactory that = (JdbcConnectionFactory) o;
    return Objects.equals(storage, that.storage)
        && Objects.equals(tableName, that.tableName)
        && Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode() {
    return Objects.hash(storage, tableName, columns);
  }
}
