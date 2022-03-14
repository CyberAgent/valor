package jp.co.cyberagent.valor.hive;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveValorOutputFormat implements HiveOutputFormat<BytesWritable, TupleWritable> {
  static final Logger LOG = LoggerFactory.getLogger(HiveValorOutputFormat.class);

  static final BytesWritable EMPTY_BYTES_WRITBLE = new BytesWritable();

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
                                          Class<? extends Writable> valueClass,
                                          boolean isCompressed, Properties tableProperties,
                                          final Progressable progressable) throws IOException {
    Configuration conf = new Configuration(jc);
    // can be removed?
    for (Map.Entry<Object, Object> prop : tableProperties.entrySet()) {
      conf.set((String) prop.getKey(), (String) prop.getValue());
    }

    String relationId = tableProperties.getProperty(ValorSerDe.HIVE_RELATION);
    HiveValorWriter writer = new HiveValorWriter(relationId, conf);
    return new RecordWriter() {
      @Override
      public void write(Writable writable) throws IOException {
        writer.write(EMPTY_BYTES_WRITBLE, (TupleWritable)writable);
      }

      @Override
      public void close(boolean b) throws IOException {
        writer.close(null);
      }
    };
  }

  @Override
  public void checkOutputSpecs(FileSystem fs, JobConf jc) throws IOException {
  }

  public org.apache.hadoop.mapred.RecordWriter<BytesWritable, TupleWritable> getRecordWriter(
      FileSystem fileSystem, JobConf jobConf, String name, Progressable progressable)
      throws IOException {
    throw new RuntimeException("Error: Hive should not invoke this method.");
  }
}
