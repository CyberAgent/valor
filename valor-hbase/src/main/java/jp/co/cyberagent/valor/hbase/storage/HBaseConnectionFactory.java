package jp.co.cyberagent.valor.hbase.storage;

import java.util.List;
import jp.co.cyberagent.valor.sdk.storage.kv.KeyValueStorageConnectionFactory;
import org.apache.hadoop.hbase.client.Connection;

public abstract class HBaseConnectionFactory extends KeyValueStorageConnectionFactory {


  protected final List<String> rowKeys;
  protected final List<String> keys;
  protected final List<String> fields;

  public HBaseConnectionFactory(HBaseStorage storage,
                                List<String> rowKeys, List<String> keys, List<String> fields) {
    super(storage);
    this.rowKeys = rowKeys;
    this.keys = keys;
    this.fields = fields;
  }

  protected Connection getConnection() {
    return ((HBaseStorage)storage).getConnection();
  }

  @Override
  public List<String> getFields() {
    return fields;
  }

  @Override
  public List<String> getRowkeyFields() {
    return rowKeys;
  }

  @Override
  public List<String> getKeyFields() {
    return keys;
  }

}
