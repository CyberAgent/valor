package jp.co.cyberagent.valor.hbase.storage;

import com.google.common.base.Objects;
import java.util.List;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import org.apache.hadoop.hbase.TableName;

public class SingleTableHBaseConnectionFactory extends HBaseConnectionFactory {

  private final TableName tableName;

  public SingleTableHBaseConnectionFactory(HBaseStorage storage,
                                           TableName tableName, List<String> rowKeys,
                                           List<String> keys, List<String> fields) {
    super(storage, rowKeys, keys, fields);
    this.tableName = tableName;
  }

  @Override
  public StorageConnection connect() throws ValorException {
    return new SingleTableHBaseConnection(tableName, getConnection());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SingleTableHBaseConnectionFactory)) {
      return false;
    }
    SingleTableHBaseConnectionFactory that = (SingleTableHBaseConnectionFactory) o;
    return Objects.equal(this.storage, that.storage) && Objects.equal(tableName, that.tableName);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName);
  }
}
