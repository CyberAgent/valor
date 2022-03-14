package jp.co.cyberagent.valor.hive;

import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.ValorConf;
import org.apache.hadoop.conf.Configuration;

/**
 *
 */
public class ValorHadoopConf implements ValorConf {

  private Configuration conf;

  public ValorHadoopConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void set(String name, String value) {
    conf.set(name, value);
  }

  @Override
  public String get(String name) {
    return conf.get(name);
  }

  @Override
  public boolean containsKey(String name) {
    return conf.get(name) != null;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return conf.iterator();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValorHadoopConf that = (ValorHadoopConf) o;
    return Objects.equals(conf, that.conf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(conf);
  }
}
