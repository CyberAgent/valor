package jp.co.cyberagent.valor.hbase.util;

import java.io.File;
import java.util.Map;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseUtil {
  static final Logger LOG = LoggerFactory.getLogger(HBaseUtil.class);

  static {
    Configuration.addDefaultResource("core-default.xml");
    Configuration.addDefaultResource("core-site.xml");
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("hbase-default.xml");
    Configuration.addDefaultResource("hbase-site.xml");
  }

  private HBaseUtil() {
  }

  public static Configuration toConfiguration(ValorConf valorConf) {


    Configuration hbaseConf = HBaseConfiguration.create();
    String confPath = valorConf.get(HBaseStorage.HBASE_CONF_KEY);
    if (confPath != null) {
      String[] confs = confPath.split(",");
      for (String c : confs) {
        File confFile = new File(c);
        if (!confFile.isFile()) {
          LOG.warn("could not find configuration file: {}", c);
          continue;
        }
        LOG.info("add configuration resource: {}", c);
        hbaseConf.addResource(new Path(confFile.toURI()));
      }
    }
    for (Map.Entry<String, String> e : valorConf) {
      hbaseConf.set(e.getKey(), e.getValue());
    }
    return hbaseConf;
  }
}
