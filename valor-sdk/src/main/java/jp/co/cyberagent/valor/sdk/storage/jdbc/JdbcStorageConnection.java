package jp.co.cyberagent.valor.sdk.storage.jdbc;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.storage.relational.RelationalStorageConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;
import jp.co.cyberagent.valor.spi.util.Pair;


/**
 *
 */
public class JdbcStorageConnection implements RelationalStorageConnection {
  private final List<JdbcStorage.ColumnDesc> columns;

  private Connection conn;

  private String tableName;

  public JdbcStorageConnection(String tableName, List<JdbcStorage.ColumnDesc> columns,
                               String jdbcUrl) throws ValorException {
    this.tableName = tableName;
    this.columns = columns;
    try {
      this.conn = DriverManager.getConnection(jdbcUrl);
    } catch (SQLException e) {
      throw new ValorException(e);
    }
  }

  @Override
  public void insert(Collection<Record> records) throws ValorException {
    String sql =
        new StringBuilder().append("INSERT INTO ").append(tableName).append("(")
            .append(columns.stream().map(c -> c.name).collect(Collectors.joining(",")))
            .append(") VALUES (")
            .append(columns.stream().map(v -> "?").collect(Collectors.joining(","))).append(")")
            .toString();

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      for (Record record : records) {
        for (int i = 0; i < columns.size(); i++) {
          JdbcStorage.ColumnDesc column = columns.get(i);
          Object v = column.type.deserialize(record.getBytes(column.name));
          stmt.setObject(i + 1, v);
        }
        stmt.execute();
        stmt.clearParameters();
      }
    } catch (SQLException e) {
      throw new ValorException(e);
    }
  }

  @Override
  public void delete(Collection<Record> records) throws ValorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Optional<Long> count(SchemaScan scan) throws ValorException {
    throw new UnsupportedOperationException("count is not supported in " + getClass().getName());
  }

  @Override
  public void update(Record record) {
    throw new UnsupportedOperationException();
  }

  @Override
  public StorageScanner getStorageScanner(StorageScan scan) throws ValorException {
    List<String> condition = new ArrayList<>();
    List<Object> parameters = new ArrayList<>();

    Pair<String, List<Object>> q = buildCondition(scan);
    condition.add(q.getFirst());
    parameters.addAll(q.getSecond());

    StringBuilder buf = new StringBuilder();
    buf.append("SELECT ");
    buf.append(columns.stream().map(i -> i.name).collect(Collectors.joining(", ")));
    buf.append(" FROM ").append(tableName).append(" WHERE ");
    buf.append(condition.stream().map(c -> String.format("(%s)", c)).collect(Collectors.joining(
        " OR ")));

    try {
      final PreparedStatement stmt = conn.prepareStatement(buf.toString());
      for (int i = 1; i <= parameters.size(); i++) {
        stmt.setObject(i, parameters.get(i - 1));
      }
      final ResultSet rs = stmt.executeQuery();
      StorageScanner scanner = new StorageScanner() {
        @Override
        public Record next() throws IOException {
          try {
            if (!rs.next()) {
              return null;
            }
            return new ResultSetRecord(rs, columns.stream().collect(Collectors.toMap(c -> c.name,
                c -> c.type)));
          } catch (SQLException e) {
            throw new IOException(e);
          }
        }

        @Override
        public void close() throws IOException {
          try {
            rs.close();
          } catch (SQLException e) {
            throw new IOException(e);
          } finally {
            try {
              stmt.close();
            } catch (SQLException e) {
              throw new IOException();
            }
          }
        }
      };
      return scanner;
    } catch (SQLException e) {
      throw new ValorException(e);
    }
  }

  @Override
  public boolean isAvailable() throws ValorException {
    try {
      // TODO isClosed() is not guaranteed whether valid or invalid a connection to a database
      return !conn.isClosed();
    } catch (SQLException e) {
      throw new ValorException();
    }
  }

  private Pair<String, List<Object>> buildCondition(StorageScan fragment) throws ValorException {
    StringBuilder attrs = new StringBuilder();
    List<Object> values = new ArrayList<>();

    for (JdbcStorage.ColumnDesc column : columns) {
      FieldComparator comparator = fragment.getFieldComparator(column.name);
      if (comparator == null) {
        continue;
      }
      if (!FieldComparator.Operator.EQUAL.equals(comparator.getOperator())) {
        continue; // TODO support more operator
      }
      attrs.append(column.name).append(" = ?").append(" AND ");
      values.add(column.type.deserialize(fragment.getStart(column.name)));
    }
    return new Pair<>(attrs.substring(0, attrs.lastIndexOf(" AND ")), values);
  }

  @Override
  public void close() throws IOException {
    try {
      conn.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
