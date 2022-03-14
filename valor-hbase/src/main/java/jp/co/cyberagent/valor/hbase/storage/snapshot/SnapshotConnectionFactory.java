package jp.co.cyberagent.valor.hbase.storage.snapshot;

import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.hbase.util.HBaseUtil;
import jp.co.cyberagent.valor.sdk.storage.kv.KeyValueStorageConnectionFactory;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class SnapshotConnectionFactory extends KeyValueStorageConnectionFactory {

  private static final String DFS_CLASS_KEY = "fs.hdfs.impl";
  private static final String LFS_CLASS_KEY = "fs.file.impl";

  private final String snapshot;
  private final List<String> fields;
  private final Configuration conf;

  public SnapshotConnectionFactory(
      HBaseSnapshotStorage storage, String snapshot, List<String> fields) {
    super(storage);
    this.snapshot = snapshot;
    this.fields = fields;
    this.conf = HBaseUtil.toConfiguration(storage.getConf());
    // workaround for service loader does not initialize DistributedFileSystem
    String clsName = this.conf.get(DFS_CLASS_KEY);
    if (clsName == null) {
      this.conf.set(DFS_CLASS_KEY, DistributedFileSystem.class.getCanonicalName());
    }
    clsName = this.conf.get(LFS_CLASS_KEY);
    if (clsName == null) {
      this.conf.set(LFS_CLASS_KEY, LocalFileSystem.class.getCanonicalName());
    }
  }

  @Override
  public StorageConnection connect() throws ValorException {
    return new SnapshotConnection(snapshot, conf);
  }

  @Override
  public List<String> getKeyFields() throws ValorException {
    return HBaseSnapshotStorage.KEY_FIELDS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnapshotConnectionFactory that = (SnapshotConnectionFactory) o;
    return Objects.equals(snapshot, that.snapshot) && Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(snapshot, fields);
  }

  @Override
  public List<String> getFields() throws ValorException {
    return fields;
  }

  @Override
  public List<String> getRowkeyFields() {
    return HBaseSnapshotStorage.KEY_FIELDS.subList(0, 1);
  }
}
