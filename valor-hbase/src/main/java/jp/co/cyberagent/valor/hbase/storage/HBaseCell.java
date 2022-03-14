package jp.co.cyberagent.valor.hbase.storage;

import jp.co.cyberagent.valor.spi.exception.InvalidFieldException;
import jp.co.cyberagent.valor.spi.storage.Record;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseCell implements Record {
  public static final String TABLE = "table";
  public static final String ROWKEY = "rowkey";
  public static final String FAMILY = "family";
  public static final String QUALIFIER = "qualifier";
  public static final String TIMESTAMP = "timestamp";
  public static final String VALUE = "value";

  private byte[] tableName;
  private byte[] rowkey;
  private byte[] family;
  private byte[] qualifier;
  private byte[] timestamp;
  private byte[] value;

  public HBaseCell(TableName name, Cell cell) {
    this.tableName = name.getName();
    this.rowkey = CellUtil.cloneRow(cell);
    this.family = CellUtil.cloneFamily(cell);
    this.qualifier = CellUtil.cloneQualifier(cell);
    this.timestamp = Bytes.toBytes(cell.getTimestamp());
    this.value = CellUtil.cloneValue(cell);
  }

  @Override
  public byte[] getBytes(String fieldName) throws InvalidFieldException {
    if (TABLE.equals(fieldName)) {
      return tableName;
    } else if (ROWKEY.equals(fieldName)) {
      return rowkey;
    } else if (FAMILY.equals(fieldName)) {
      return family;
    } else if (QUALIFIER.equals(fieldName)) {
      return qualifier;
    } else if (TIMESTAMP.equals(fieldName)) {
      return timestamp;
    } else if (VALUE.equals(fieldName)) {
      return value;
    } else {
      throw new InvalidFieldException(fieldName, "HBase");
    }
  }

  @Override
  public void setBytes(String fieldName, byte[] v) throws InvalidFieldException {
    if (TABLE.equals(fieldName)) {
      tableName = v;
    } else if (ROWKEY.equals(fieldName)) {
      rowkey = v;
    } else if (FAMILY.equals(fieldName)) {
      family = v;
    } else if (QUALIFIER.equals(fieldName)) {
      qualifier = v;
    } else if (TIMESTAMP.equals(fieldName)) {
      timestamp = v;
    } else if (VALUE.equals(fieldName)) {
      value = v;
    } else {
      throw new InvalidFieldException(fieldName, "HBase");
    }
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("Table: ").append(Bytes.toString(tableName));
    buf.append(" Row: ").append(Bytes.toStringBinary(rowkey));
    buf.append(" Family: ").append(Bytes.toStringBinary(family));
    buf.append(" Qualifier: ").append(Bytes.toStringBinary(qualifier));
    buf.append(" Timestamp: ").append(Bytes.toStringBinary(timestamp));
    buf.append(" Value: ").append(Bytes.toStringBinary(value));
    return buf.toString();
  }
}
