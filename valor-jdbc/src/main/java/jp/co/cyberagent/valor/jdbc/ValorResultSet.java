package jp.co.cyberagent.valor.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import javax.sql.rowset.RowSetMetaDataImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.Tuple;

public class ValorResultSet implements ResultSet {

  static ValueConverter<String> stringConverter = new ValueConverter<>(String.class);
  static ValueConverter<Boolean> booleanConverter = new ValueConverter<>(Boolean.class);
  static ValueConverter<Byte> byteConverter = new ValueConverter<>(Byte.class);
  static ValueConverter<byte[]> bytesConverter = new ValueConverter<>(byte[].class);
  static ValueConverter<Integer> integerConverter = new ValueConverter<>(Integer.class);
  static ValueConverter<Long> longConverter = new ValueConverter<>(Long.class);
  static ValueConverter<Short> shortConverter = new ValueConverter<>(Short.class);
  static ValueConverter<Double> doubleConverter = new ValueConverter<>(Double.class);
  static ValueConverter<Float> floatConverter = new ValueConverter<>(Float.class);
  static ValueConverter<Object> objectConverter = new ValueConverter<>(Object.class);
  private final TupleScanner runner;
  private final ResultSetMetaData metadata;
  private Tuple tuple;

  public ValorResultSet(TupleScanner runner) throws SQLException {
    this.runner = runner;
    this.metadata = buildMetaData(runner.getItems());
  }

  private ResultSetMetaData buildMetaData(List<ProjectionItem> items) throws SQLException {
    RowSetMetaDataImpl metaDataImpl = new RowSetMetaDataImpl();
    metaDataImpl.setColumnCount(items.size());
    for (int i = 1; i <= items.size(); i++) {
      ProjectionItem item = items.get(i - 1);
      metaDataImpl.setColumnName(i, item.getAlias());
      // TODO: When an alias can be set, implement here with an appropriate label
      metaDataImpl.setColumnLabel(i, item.getAlias());
      metaDataImpl.setColumnType(i, toSqlType(item.getValue().getType().getRepresentedClass()));
    }
    return metaDataImpl;
  }

  private int toSqlType(Class<?> cls) {
    if (List.class.isAssignableFrom(cls)) {
      return Types.ARRAY;
    }
    if (Long.class.isAssignableFrom(cls)) {
      return Types.BIGINT;
    }
    //  static int BINARY;
    //  static int BIT
    //  static int  BLOB
    if (Boolean.class.isAssignableFrom(cls)) {
      return Types.BOOLEAN;
    }
    //  static int  CHAR
    //  static int  CLOB
    //  static int  DATALINK
    //  static int  DATE
    //  static int  DECIMAL
    //  static int  DISTINCT
    if (Double.class.isAssignableFrom(cls)) {
      return Types.DOUBLE;
    }
    if (Float.class.isAssignableFrom(cls)) {
      return Types.FLOAT;
    }
    if (Integer.class.isAssignableFrom(cls)) {
      return Types.INTEGER;
    }
    //  static int  JAVA_OBJECT
    //  static int  LONGNVARCHAR
    //  static int  LONGVARBINARY
    //  static int  LONGVARCHAR
    //  static int  NCHAR
    //  static int  NCLOB
    //  static int  NULL
    //  static int  NUMERIC
    //  static int  NVARCHAR
    //  static int  OTHER
    //  static int  REAL
    //  static int  REF
    //  static int  ROWID
    //  static int  SMALLINT
    //  static int  SQLXML
    //  static int  STRUCT
    //  static int  TIME
    //  static int  TIMESTAMP
    //  static int  TINYINT
    // static int VARBINARY
    if (String.class.isAssignableFrom(cls)) {
      return Types.VARCHAR;
    }
    return Types.OTHER;
  }

  @Override
  public boolean next() throws SQLException {
    try {
      tuple = runner.next();
    } catch (ValorException e) {
      throw new SQLException(e);
    }
    return tuple != null;
  }

  @Override
  public void close() throws SQLException {
    try {
      runner.close();
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("unwrap");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("isWrapperFor");
  }

  @Override
  public boolean wasNull() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("wasNull is not implemented");
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getString(colName);
  }

  @Override
  public String getString(String columnLabel) throws SQLException {
    return stringConverter.extractAndCast(tuple, columnLabel);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getBoolean(colName);
  }

  @Override
  public boolean getBoolean(String columnLabel) throws SQLException {
    return booleanConverter.extractAndCast(tuple, columnLabel);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getByte(colName);
  }

  @Override
  public byte getByte(String columnLabel) throws SQLException {
    return byteConverter.extractAndCast(tuple, columnLabel);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getShort(colName);
  }

  @Override
  public short getShort(String columnLabel) throws SQLException {
    return shortConverter.extractAndCast(tuple, columnLabel);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getInt(colName);
  }

  @Override
  public int getInt(String columnLabel) throws SQLException {
    return integerConverter.extractAndCast(tuple, columnLabel);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getLong(colName);
  }

  @Override
  public long getLong(String columnLabel) throws SQLException {
    return longConverter.extractAndCast(tuple, columnLabel);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getFloat(colName);
  }

  @Override
  public float getFloat(String columnLabel) throws SQLException {
    return floatConverter.extractAndCast(tuple, columnLabel);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getDouble(colName);
  }

  @Override
  public double getDouble(String columnLabel) throws SQLException {
    return doubleConverter.extractAndCast(tuple, columnLabel);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getBigDecimal(colName);
  }

  @Override
  public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getBigDecimal() is not implemented");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getBigDecimal() is not implemented");
  }

  @Override
  public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getBigDecimal() is not implemented");
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getBytes(colName);
  }

  @Override
  public byte[] getBytes(String columnLabel) throws SQLException {
    return bytesConverter.extractAndCast(tuple, columnLabel);
  }

  @Override
  public Date getDate(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getDate(colName);
  }

  @Override
  public Date getDate(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Date getDate(int columnIndex, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDate() is not implemented");
  }

  @Override
  public Date getDate(String columnLabel, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getDate() is not implemented");
  }

  @Override
  public Time getTime(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getTime(colName);
  }

  @Override
  public Time getTime(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Time getTime(int columnIndex, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getTime() is not implemented");
  }

  @Override
  public Time getTime(String columnLabel, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getTime() is not implemented");
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getTimestamp(colName);
  }

  @Override
  public Timestamp getTimestamp(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getTimestamp() is not implemented");
  }

  @Override
  public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getTimestamp() is not implemented");
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getAsciiStream");
  }

  @Override
  public InputStream getAsciiStream(String columnLabel) throws SQLException {
    throw new UnsupportedOperationException("getAsciiStream");
  }

  @Override
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getUnicodeStream");
  }

  @Override
  public InputStream getUnicodeStream(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getUnicodeStream");
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getBinaryStream");
  }

  @Override
  public InputStream getBinaryStream(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getBinaryStream");
  }

  @Override
  public Reader getNCharacterStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getNCharacterStream() is not implemented");
  }

  @Override
  public Reader getNCharacterStream(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getNCharacterStream() is not implemented");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getWarnings");
  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("clearWarnings");
  }

  @Override
  public String getCursorName() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getCursorName");
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return this.metadata;
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    String colName = metadata.getColumnName(columnIndex);
    return getObject(colName);
  }

  @Override
  public Object getObject(String columnLabel) throws SQLException {
    return objectConverter.extractAndCast(tuple, columnLabel);
  }

  @Override
  public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getObject() is not implemented");
  }

  @Override
  public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getObject() is not implemented");
  }

  @Override
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getObject() is not implemented");
  }

  @Override
  public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getObject() is not implemented");
  }

  @Override
  public int findColumn(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("findColumn() is not implemented");
  }

  @Override
  public Reader getCharacterStream(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getCharacterStream() is not implemented");
  }

  @Override
  public Reader getCharacterStream(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getCharacterStream() is not implemented");
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("isBeforeFirst() is not implemented");
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("isAfterLast() is not implemented");
  }

  @Override
  public boolean isFirst() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("isFirst() is not implemented");
  }

  @Override
  public boolean isLast() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("isLast() is not implemented");
  }

  @Override
  public void beforeFirst() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("beforeFirst() is not implemented");
  }

  @Override
  public void afterLast() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("afterLast() is not implemented");
  }

  @Override
  public boolean first() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("first() is not implemented");
  }

  @Override
  public boolean last() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("last() is not implemented");
  }

  @Override
  public int getRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getRow() is not implemented");
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("absolute() is not implemented");
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("relative() is not implemented");
  }

  @Override
  public boolean previous() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("previous() is not implemented");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getFetchDirection() is not implemented");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setFetchDirection() is not implemented");
  }

  @Override
  public int getFetchSize() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getFetchSize() is not implemented");
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("setFetchSize() is not implemented");
  }

  @Override
  public int getType() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getType() is not implemented");
  }

  @Override
  public int getConcurrency() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getConcurrency() is not implemented");
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("rowUpdated() is not implemented");
  }

  @Override
  public boolean rowInserted() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("rowInserted() is not implemented");
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("rowDeleted() is not implemented");
  }

  @Override
  public void updateNull(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNull() is not implemented");
  }

  @Override
  public void updateNull(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNull() is not implemented");
  }

  @Override
  public void updateBoolean(int columnIndex, boolean x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBoolean() is not implemented");
  }

  @Override
  public void updateBoolean(String columnLabel, boolean x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBoolean() is not implemented");
  }

  @Override
  public void updateByte(int columnIndex, byte x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateByte() is not implemented");
  }

  @Override
  public void updateByte(String columnLabel, byte x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateByte() is not implemented");
  }

  @Override
  public void updateShort(int columnIndex, short x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateShort() is not implemented");
  }

  @Override
  public void updateShort(String columnLabel, short x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateShort() is not implemented");
  }

  @Override
  public void updateInt(int columnIndex, int x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateInt() is not implemented");
  }

  @Override
  public void updateInt(String columnLabel, int x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateInt() is not implemented");
  }

  @Override
  public void updateLong(int columnIndex, long x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateLong() is not implemented");
  }

  @Override
  public void updateLong(String columnLabel, long x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateLong() is not implemented");
  }

  @Override
  public void updateFloat(int columnIndex, float x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateFloat() is not implemented");
  }

  @Override
  public void updateFloat(String columnLabel, float x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateFloat() is not implemented");
  }

  @Override
  public void updateDouble(int columnIndex, double x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateDouble() is not implemented");
  }

  @Override
  public void updateDouble(String columnLabel, double x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateDouble() is not implemented");
  }

  @Override
  public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBigDecimal() is not implemented");
  }

  @Override
  public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBigDecimal() is not implemented");
  }

  @Override
  public void updateString(int columnIndex, String x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateString() is not implemented");
  }

  @Override
  public void updateString(String columnLabel, String x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateString() is not implemented");
  }

  @Override
  public void updateBytes(int columnIndex, byte[] x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBytes() is not implemented");
  }

  @Override
  public void updateBytes(String columnLabel, byte[] x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBytes() is not implemented");
  }

  @Override
  public void updateDate(int columnIndex, Date x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateDate() is not implemented");
  }

  @Override
  public void updateDate(String columnLabel, Date x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateDate() is not implemented");
  }

  @Override
  public void updateTime(int columnIndex, Time x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateTime() is not implemented");
  }

  @Override
  public void updateTime(String columnLabel, Time x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateTime() is not implemented");
  }

  @Override
  public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateTimestamp() is not implemented");
  }

  @Override
  public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateTimestamp() is not implemented");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateAsciiStream() is not implemented");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateAsciiStream() is not implemented");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateAsciiStream() is not implemented");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateAsciiStream() is not implemented");
  }

  @Override
  public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateAsciiStream() is not implemented");
  }

  @Override
  public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateAsciiStream() is not implemented");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBinaryStream() is not implemented");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBinaryStream() is not implemented");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBinaryStream() is not implemented");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBinaryStream() is not implemented");
  }

  @Override
  public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBinaryStream() is not implemented");
  }

  @Override
  public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBinaryStream() is not implemented");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateCharacterStream() is not implemented");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, int length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateCharacterStream() is not implemented");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateCharacterStream() is not implemented");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateCharacterStream() is not implemented");
  }

  @Override
  public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateCharacterStream() is not implemented");
  }

  @Override
  public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateCharacterStream() is not implemented");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNCharacterStream() is not implemented");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNCharacterStream() is not implemented");
  }

  @Override
  public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNCharacterStream() is not implemented");
  }

  @Override
  public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNCharacterStream() is not implemented");
  }

  @Override
  public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateObject() is not implemented");
  }

  @Override
  public void updateObject(int columnIndex, Object x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateObject() is not implemented");
  }

  @Override
  public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateObject() is not implemented");
  }

  @Override
  public void updateObject(String columnLabel, Object x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateObject() is not implemented");
  }

  @Override
  public void insertRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("insertRow() is not implemented");
  }

  @Override
  public void updateRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateRow() is not implemented");
  }

  @Override
  public void deleteRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("deleteRow() is not implemented");
  }

  @Override
  public void refreshRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("refreshRow() is not implemented");
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("cancelRowUpdates() is not implemented");
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("moveToInsertRow() is not implemented");
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("moveToCurrentRow() is not implemented");
  }

  @Override
  public Statement getStatement() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getStatement() is not implemented");
  }

  @Override
  public Ref getRef(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getRef() is not implemented");
  }

  @Override
  public Ref getRef(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getRef() is not implemented");
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getBlob() is not implemented");
  }

  @Override
  public Blob getBlob(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getBlob() is not implemented");
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getClob() is not implemented");
  }

  @Override
  public Clob getClob(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getClob() is not implemented");
  }

  @Override
  public Array getArray(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getArray() is not implemented");
  }

  @Override
  public Array getArray(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getArray() is not implemented");
  }

  @Override
  public URL getURL(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getURL() is not implemented");
  }

  @Override
  public URL getURL(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getURL() is not implemented");
  }

  @Override
  public RowId getRowId(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getRowId() is not implemented");
  }

  @Override
  public RowId getRowId(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getRowId() is not implemented");
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getNClob() is not implemented");
  }

  @Override
  public NClob getNClob(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getNClob() is not implemented");
  }


  @Override
  public SQLXML getSQLXML(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSQLXML() is not implemented");
  }

  @Override
  public SQLXML getSQLXML(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getSQLXML() is not implemented");
  }


  @Override
  public void updateRef(int columnIndex, Ref x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateRef() is not implemented");
  }

  @Override
  public void updateRef(String columnLabel, Ref x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateRef() is not implemented");
  }

  @Override
  public void updateBlob(int columnIndex, Blob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBlob() is not implemented");
  }

  @Override
  public void updateBlob(String columnLabel, Blob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBlob() is not implemented");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBlob() is not implemented");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBlob() is not implemented");
  }

  @Override
  public void updateBlob(int columnIndex, InputStream inputStream, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBlob() is not implemented");
  }

  @Override
  public void updateBlob(String columnLabel, InputStream inputStream, long length)
      throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateBlob() is not implemented");
  }


  @Override
  public void updateClob(int columnIndex, Clob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateClob() is not implemented");
  }

  @Override
  public void updateClob(String columnLabel, Clob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateClob() is not implemented");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateClob() is not implemented");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateClob() is not implemented");
  }

  @Override
  public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateClob() is not implemented");
  }

  @Override
  public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateClob() is not implemented");
  }


  @Override
  public void updateArray(int columnIndex, Array x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateArray() is not implemented");
  }

  @Override
  public void updateArray(String columnLabel, Array x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateArray() is not implemented");
  }


  @Override
  public void updateRowId(int columnIndex, RowId x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateRowId() is not implemented");
  }

  @Override
  public void updateRowId(String columnLabel, RowId x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateRowId() is not implemented");
  }

  @Override
  public int getHoldability() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getHoldability() is not implemented");
  }

  @Override
  public boolean isClosed() throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("isClosed() is not implemented");
  }

  @Override
  public void updateNString(int columnIndex, String x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNString() is not implemented");
  }

  @Override
  public void updateNString(String columnLabel, String x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNString() is not implemented");
  }


  @Override
  public void updateNClob(int columnIndex, NClob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNClob() is not implemented");
  }

  @Override
  public void updateNClob(String columnLabel, NClob x) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNClob() is not implemented");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNClob() is not implemented");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNClob() is not implemented");
  }

  @Override
  public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNClob() is not implemented");
  }

  @Override
  public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateNClob() is not implemented");
  }


  @Override
  public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateSQLXML() is not implemented");
  }

  @Override
  public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("updateSQLXML() is not implemented");
  }

  @Override
  public String getNString(int columnIndex) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getNString() is not implemented");
  }

  @Override
  public String getNString(String columnLabel) throws SQLException {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("getNString() is not implemented");
  }


  static class ValueConverter<T> {

    private final Class<T> cls;

    public ValueConverter(Class<T> cls) {
      this.cls = cls;
    }

    protected T extractAndCast(Tuple t, String attr) {
      if (t == null) {
        throw new IllegalStateException("no tuple to read");
      }
      Object val = t.getAttribute(attr);
      if (val == null) {
        return null;
      }
      return cls.cast(val);
    }
  }
}
