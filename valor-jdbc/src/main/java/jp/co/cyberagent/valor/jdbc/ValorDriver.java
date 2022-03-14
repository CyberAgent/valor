package jp.co.cyberagent.valor.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValorDriver implements Driver {

  private static final String JDBC_URL_HEADER = "jdbc:valor:";
  static Logger LOG = LoggerFactory.getLogger(ValorDriver.class);

  static {
    try {
      DriverManager.registerDriver(new ValorDriver());
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to register "
          + ValorDriver.class.getCanonicalName(), e);
    }
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    ValorConf conf = new ValorPropertiesConfig(info);
    ValorContext context = StandardContextFactory.create(conf);
    return new ValorJdbcConnection(context);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(JDBC_URL_HEADER);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMajorVersion() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getMinorVersion() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean jdbcCompliant() {
    throw new UnsupportedOperationException();
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }


}
