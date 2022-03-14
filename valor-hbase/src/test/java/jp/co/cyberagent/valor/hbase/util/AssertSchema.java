package jp.co.cyberagent.valor.hbase.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


import java.util.List;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.storage.Record;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class AssertSchema {

  public static void dump(Connection conn, String table) throws Exception {
    try (Table htable = conn.getTable(TableName.valueOf(table))) {
      ResultScanner scanner = htable.getScanner(new Scan());
      Result r;
      while ((r = scanner.next()) != null) {
        for (Cell cell : r.listCells()) {
          System.out.println(cell);
        }
      }
    }
  }

  public static void assertHasTuples(Connection conn, String tableName, Schema schema,
                                          Tuple... tuples) throws Exception {
    for (Tuple tuple : tuples) {
      assertHasTuple(conn, tableName, schema, tuple);
    }
  }

  @SuppressWarnings("unchecked")
  public static void assertHasTuple(Connection conn, String tableName, Schema schema,
                                         Tuple tuple) throws Exception {
    List<Record> kvws = schema.serialize(tuple);
    for (Record kvw : kvws) {
      Get get = new Get(kvw.getBytes(HBaseCell.ROWKEY));
      get.addColumn(kvw.getBytes(HBaseCell.FAMILY), kvw.getBytes(HBaseCell.QUALIFIER));
      try (Table htable = conn.getTable(TableName.valueOf(tableName))) {
        Result value = htable.get(get);
        assertNotNull(value);
        assertEquals(1, value.listCells().size());
        byte[] actual = CellUtil.cloneValue(value.listCells().get(0));
        assertArrayEquals(kvw.getBytes(HBaseCell.VALUE), actual);
      }
    }
  }

  public static void assertNotHaveTuple(Connection conn, String tableName, Schema schema,
                                        Tuple tuple) throws Exception {
    List<Record> kvws = schema.serialize(tuple);
    for (Record kvw : kvws) {
      Get get = new Get(kvw.getBytes(HBaseCell.ROWKEY));
      get.addColumn(kvw.getBytes(HBaseCell.FAMILY), kvw.getBytes(HBaseCell.QUALIFIER));
      try (Table htable = conn.getTable(TableName.valueOf(tableName))) {
        Result value = htable.get(get);
        assertTrue(value.isEmpty());
      }
    }
  }

  public static void assertScanResultEquals(Tuple expected, Tuple actual) {
    assertEquals(expected.getAttributeNames().size(), actual.getAttributeNames().size());
    for (String attr : expected.getAttributeNames()) {
      Object expectedValue = expected.getAttribute(attr);
      Object actualValue = actual.getAttribute(attr);
      assertEquals(expectedValue, actualValue,
          String.format("%s differs: expected <%s>, but was <%s>",
              attr, expectedValue, actualValue));
    }
  }
}
