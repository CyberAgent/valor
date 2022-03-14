package jp.co.cyberagent.valor.sdk.storage.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.storage.relational.RelationalStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfParam;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageFactory;
import jp.co.cyberagent.valor.spi.util.Pair;


public class JdbcStorage extends RelationalStorage {

  public static final ValorConfParam JDBC_URL = new ValorConfParam("valor.jdbc.url", null);

  public static final ValorConfParam JDBC_TABLE_NAME = new ValorConfParam("valor.jdbc.table", null);

  public static final String NAME = "jdbc";
  private String tableName;
  private List<ColumnDesc> columns;

  public JdbcStorage(ValorConf conf) throws ValorException {
    super(conf);
    this.tableName = JDBC_TABLE_NAME.get(conf);
    try (Connection conn = DriverManager.getConnection(JDBC_URL.get(conf))) {
      columns = describe(conn, tableName);
    } catch (SQLException e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected StorageConnectionFactory getConnectionFactory(
      Relation relation,
      SchemaDescriptor schemaDescriptor) {
    return new JdbcConnectionFactory(this, tableName, columns);
  }

  public static AttributeType toValorType(int jdbcType) {
    switch (jdbcType) {
      case Types.VARCHAR:
        return StringAttributeType.INSTANCE;
      default:
        throw new IllegalArgumentException(jdbcType + " is not supported currently");
    }
  }

  private List<ColumnDesc> describe(Connection conn, String tableName) throws ValorException {
    List<Pair<String, AttributeType>> columns = new ArrayList<>();
    try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT * FROM "
        + tableName + " FETCH FIRST 1 ROWS ONLY")) {
      ResultSetMetaData md = rs.getMetaData();
      for (int i = 1; i <= md.getColumnCount(); i++) {
        int type = md.getColumnType(i);
        columns.add(new Pair<>(md.getColumnName(i), toValorType(type)));
      }
    } catch (SQLException e) {
      throw new ValorException(e);
    }
    List<String> pkeys = new ArrayList<>();
    try (ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, tableName)) {
      while (rs.next()) {
        pkeys.add(rs.getString("COLUMN_NAME"));
      }
    } catch (SQLException e) {
      throw new ValorException(e);
    }
    return columns.stream().map(c -> new ColumnDesc(c.getFirst(), c.getSecond(),
        pkeys.contains(c))).collect(Collectors.toList());
  }

  public static class Factory extends StorageFactory {
    @Override
    protected Storage doCreate(ValorConf config) {
      try {
        return new JdbcStorage(config);
      } catch (ValorException e) {
        throw new IllegalArgumentException(e);
      }
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Storage> getProvidedClass() {
      return JdbcStorage.class;
    }
  }

  public static class ColumnDesc {
    public final String name;
    public final boolean key;
    public final AttributeType type;

    public ColumnDesc(String name, AttributeType type, boolean key) {
      this.name = name;
      this.type = type;
      this.key = key;
    }
  }
}
