package jp.co.cyberagent.valor.hbase.storage;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;

/**
 *
 */
public class HBaseScanner extends StorageScanner {
  private Table table;
  private ResultScanner scanner;
  private Iterator<Cell> currentCells;

  public HBaseScanner(Table table, ResultScanner scanner) {
    this.table = table;
    this.scanner = scanner;
    this.currentCells = Collections.EMPTY_LIST.iterator();
  }

  @Override
  public Record next() throws IOException {
    while (currentCells != null) {
      if (currentCells.hasNext()) {
        return new HBaseCell(table.getName(), currentCells.next());
      }
      Result nextCells = scanner.next();
      currentCells = nextCells == null ? null : nextCells.listCells().iterator();
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    try {
      scanner.close();
    } finally {
      table.close();
    }
  }
}
