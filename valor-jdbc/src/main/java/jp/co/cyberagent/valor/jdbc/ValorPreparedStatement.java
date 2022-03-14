package jp.co.cyberagent.valor.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import jp.co.cyberagent.valor.spi.plan.model.ParameterizedStatement;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;

public class ValorPreparedStatement extends ValorJdbcStatementBase implements PreparedStatement {

  private final ParameterizedStatement query;

  public ValorPreparedStatement(ParameterizedStatement query, ValorJdbcConnection conn) {
    super(conn);
    this.query = query;
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    return doExecuteSelect((RelationScan) query.getStatement());
  }

  @Override
  public int executeUpdate() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("executeUpdate() is not implemented");
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    setObject(parameterIndex, null);
  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    setObject(parameterIndex, null);
  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setURL() is not implemented");
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    throw new UnsupportedOperationException("setDate() is not implemented");
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setTime() is not implemented");
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setTimestamp() is not implemented");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setAsciiStream() is not implemented");
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setAsciiStream() is not implemented");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setBinaryStream() is not implemented");
  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setBinaryStream() is not implemented");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setCharacterStream() is not implemented");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setCharacterStream() is not implemented");
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setCharacterStream() is not implemented");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setNCharacterStream() is not implemented");
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setNCharacterStream() is not implemented");
  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    throw new UnsupportedOperationException();
  }


  @Override
  public void clearParameters() throws SQLException {
    this.query.clearParameter();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    setObject(parameterIndex, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    this.query.setParameter(parameterIndex - 1, x);
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setObject() is not implemented");
  }

  @Override
  public boolean execute() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addBatch() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setBlob() is not implemented");
  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setBlob() is not implemented");
  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setClob() is not implemented");
  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setClob() is not implemented");
  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setNClob() is not implemented");
  }


  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setNClob() is not implemented");
  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setNClob() is not implemented");
  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setRowId() is not implemented");
  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setNString() is not implemented");
  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setSQLXML() is not implemented");
  }


}
