package jp.co.cyberagent.valor.hive;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScanner;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HiveValorReader implements RecordReader<NullWritable, TupleWritable> {
  private static final Logger LOG = LoggerFactory.getLogger(HiveValorReader.class);
  private final StorageConnection sc;

  private final ValorConnection vc;

  private final SchemaScanner scanner;

  private final Relation relation;

  @SuppressWarnings("unchecked")
  public HiveValorReader(ValorSplit ss, Configuration conf) throws ValorException, IOException {
    ValorContext context = StandardContextFactory.create(new ValorHadoopConf(conf));
    vc = ValorConnectionFactory.create(context);
    LOG.info(vc.getClass() + " is initiated");
    this.relation = vc.findRelation(ss.getRelationId());
    Schema schema = vc.findSchema(ss.getRelationId(), ss.getSchemaId());
    this.sc = schema.getConnectionFactory().connect();

    PredicativeExpression condition = ValorSerDe.extractCondition(relation, conf);
    SchemaScan scan = new SchemaScan(relation, schema, condition);
    scan.add(ss.getScanFragment());
    this.scanner = schema.getScanner(scan, sc);
  }

  @Override
  public void close() throws IOException {
    Optional<IOException> e = Arrays.asList(scanner, sc, vc).stream().map(c -> {
      try {
        c.close();
      } catch (IOException ioe) {
        LOG.warn("failed to close " + c, ioe);
        return ioe;
      }
      return null;
    }).filter(ioe -> ioe != null).findAny();
    if (e.isPresent()) {
      throw e.get();
    }
  }

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public TupleWritable createValue() {
    return new TupleWritable();
  }

  @Override
  public long getPos() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public float getProgress() throws IOException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean next(NullWritable key, TupleWritable value) throws IOException {
    Tuple tuple = null;
    try {
      tuple = this.scanner.next();
    } catch (ValorException e) {
      throw new IOException(e);
    }
    if (tuple == null) {
      return false;
    }

    TupleWritable nextTuple = new TupleWritable(relation, tuple);
    try (ByteArrayOutputStream daos = new ByteArrayOutputStream(); DataOutputStream dos =
        new DataOutputStream(daos)) {
      nextTuple.write(dos);
      byte[] bytes = daos.toByteArray();
      try (ByteArrayInputStream dios = new ByteArrayInputStream(bytes); DataInputStream dis =
          new DataInputStream(dios)) {
        value.readFields(dis);
      }
    }
    return true;
  }
}
