package jp.co.cyberagent.valor.hbase.storage;

import org.apache.hadoop.hbase.client.Scan;

public class HBaseScan {
  private byte[] tableName;
  private Scan scan;

  /**
   * @param scan included scan
   */
  public HBaseScan(byte[] tableName, Scan scan) {
    this.tableName = tableName;
    this.scan = scan;
  }

  public byte[] getTableName() {
    return this.tableName;
  }

  public Scan getScan() {
    return this.scan;
  }
}
