package jp.co.cyberagent.valor.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveValorWriter extends RecordWriter<BytesWritable, TupleWritable> {

  static Logger LOG = LoggerFactory.getLogger(HiveValorWriter.class);

  public static final String BATCH_SIZE = "valor.hive.batch.size";
  public static final int DEFAULT_BATCH_SIZE = 100;

  private final ValorConnection vc;
  private final Relation relation;
  private final List<Tuple> buf;
  private final int batchSize;

  public HiveValorWriter(String relationId, Configuration conf) throws IOException {
    ValorContext context = StandardContextFactory.create(new ValorHadoopConf(conf));
    this.batchSize = conf.getInt(BATCH_SIZE, DEFAULT_BATCH_SIZE);
    this.buf = new ArrayList<>(batchSize);
    this.vc = ValorConnectionFactory.create(context);
    LOG.info(vc.getClass() + " is initiated");
    try {
      this.relation = vc.findRelation(relationId);
    } catch (ValorException e) {
      throw new IOException(e);
    }
  }


  @Override
  public void write(BytesWritable bytesWritable, TupleWritable tupleWritable) throws IOException {
    Tuple tuple = tupleWritable.convertFromWritable(relation);
    buf.add(tuple);
    if (buf.size() >= batchSize) {
      try {
        vc.insert(relation.getRelationId(), buf);
      } catch (ValorException e) {
        throw new IOException(e);
      }
      buf.clear();
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext) throws IOException {
    if (buf.size() > 0) {
      try {
        vc.insert(relation.getRelationId(), buf);
      } catch (ValorException e) {
        throw new IOException(e);
      }
    }
    vc.close();
  }
}
