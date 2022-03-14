package jp.co.cyberagent.valor.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;

public class ValorJdbcStatement extends ValorJdbcStatementBase {

  public ValorJdbcStatement(ValorJdbcConnection connection) {
    super(connection);
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    RelationScan query = (RelationScan) connection.parse(sql);
    return doExecuteSelect(query);
  }

}
