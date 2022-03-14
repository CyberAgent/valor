package jp.co.cyberagent.valor.hbase.storage;

import java.io.IOException;
import jp.co.cyberagent.valor.sdk.storage.kv.KeyValueStorageConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class HBaseConnection implements KeyValueStorageConnection {

  static final Logger LOG = LoggerFactory.getLogger(HBaseConnection.class);

  protected Connection connection;

  public HBaseConnection(Connection conn) throws ValorException {
    this.connection = conn;
  }

  public Connection getConnection() {
    return connection;
  }

  @Override
  public void close() throws IOException {
    // nothing to do for reusing connection
  }

  @Override
  public boolean isAvailable() throws ValorException {
    try (Admin admin = connection.getAdmin()) {
      TableName[] tableNames = admin.listTableNames();
      return tableNames.length != 0;
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

}
