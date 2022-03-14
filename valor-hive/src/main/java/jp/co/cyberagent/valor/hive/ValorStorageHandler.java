package jp.co.cyberagent.valor.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValorStorageHandler extends DefaultStorageHandler
    implements HiveStoragePredicateHandler {

  static Logger LOG = LoggerFactory.getLogger(ValorStorageHandler.class);

  private Configuration conf;

  public ValorStorageHandler() {
    this.conf = new Configuration();
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass() {
    return HiveValorInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass() {
    return HiveValorOutputFormat.class;
  }

  @Override
  public Class<? extends AbstractSerDe> getSerDeClass() {
    return ValorSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook() {
    return null;
  }

  @Override
  public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
    tableDesc.getProperties().forEach((key, value) -> {
      if (key instanceof String) {
        String k = (String) key;
        if (k.startsWith("valor")) {
          jobConf.set(k, (String) value);
        }
      }
    });
    conf = new Configuration(jobConf);
  }

  @Override
  public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
    return new DefaultHiveAuthorizationProvider();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer,
                                                ExprNodeDesc exprNodeDesc) {
    DecomposedPredicate pred = new DecomposedPredicate();
    pred.pushedPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    pred.residualPredicate = (ExprNodeGenericFuncDesc) exprNodeDesc;
    return pred;
  }
}
