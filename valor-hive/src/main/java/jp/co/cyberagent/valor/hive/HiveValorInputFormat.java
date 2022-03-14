package jp.co.cyberagent.valor.hive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.hbase.HBasePlugin;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.plan.NaivePrimitivePlanner;
import jp.co.cyberagent.valor.sdk.plan.PrimitivePlanner;
import jp.co.cyberagent.valor.sdk.plan.SimplePlan;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.zookeeper.ZookeeperPlugin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveValorInputFormat extends HiveInputFormat<NullWritable, TupleWritable> {
  static final Logger LOG = LoggerFactory.getLogger(HiveValorInputFormat.class);

  @Override
  public RecordReader<NullWritable, TupleWritable> getRecordReader(InputSplit split,
                                                                   JobConf jobConf,
                                                                   final Reporter reporter)
      throws IOException {
    try {
      ValorSplit vs = (ValorSplit) split;
      return new HiveValorReader(vs, jobConf);
    } catch (Exception e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new IOException(e);
    }
  }

  public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
    ValorContext context = StandardContextFactory.defaultContextFactory
        .apply(new ValorHadoopConf(new Configuration(jobConf)));
    LOG.info("loading plugins");
    context.installPlugin(new HBasePlugin());
    context.installPlugin(new ZookeeperPlugin());

    Path dummyPath = FileInputFormat.getInputPaths(jobConf)[0];
    String relationId = jobConf.get(ValorSerDe.HIVE_RELATION);
    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      Relation relation = conn.findRelation(relationId);
      Collection<Schema> schemas = conn.listSchemas(null, relation.getRelationId());
      List<ProjectionItem> items = relation.getAttributes().stream()
          .map(a -> new AttributeNameExpression(a.name(), a.type()))
          .map(a -> new ProjectionItem(a, a.getName()))
          .collect(Collectors.toList());
      PredicativeExpression condition = ValorSerDe.extractCondition(relation, jobConf);

      PrimitivePlanner planner = new NaivePrimitivePlanner();
      ScanPlan plan = relation.getSchemaHandler().plan(conn, schemas, items, condition, planner);
      if (!(plan instanceof SimplePlan)) {
        throw new ValorException("scan using multiple schema is not supported: " + plan);
      }

      SchemaScan choice = ((SimplePlan)plan).getScan();
      if (choice == null) {
        throw new ValorRuntimeException("failed to find appropriate schema for " + relationId);
      }

      List<StorageScan> scanSplits = new ArrayList<>();
      try (StorageConnection sc = conn.createConnection(choice.getSchema())) {
        for (StorageScan ss : choice.getFragments()) {
          sc.split(ss).forEach(scanSplits::add);
        }
      }
      InputSplit[] splits = new InputSplit[scanSplits.size()];
      LOG.info(splits.length + " splits");
      for (int i = 0; i < splits.length; i++) {
        splits[i] = new ValorSplit(
            dummyPath, relationId, choice.getSchema().getSchemaId(), scanSplits.get(i));
        LOG.info("split {} : {}", i, splits[i].toString());
      }
      return splits;
    } catch (ValorException e) {
      throw new IOException(e);
    }
  }

}
