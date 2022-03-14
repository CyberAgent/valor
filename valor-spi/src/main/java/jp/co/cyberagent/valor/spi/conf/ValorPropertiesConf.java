package jp.co.cyberagent.valor.spi.conf;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import jp.co.cyberagent.valor.spi.ValorConf;

/**
 *
 */
public class ValorPropertiesConf implements ValorConf {

  private Properties properties;

  public ValorPropertiesConf(Properties props) {
    this.properties = props;
  }

  @Override
  public void set(String name, String value) {
    properties.setProperty(name, value);
  }

  @Override
  public String get(String name) {
    return properties.getProperty(name);
  }

  @Override
  public boolean containsKey(String name) {
    return properties.containsKey(name);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    final Iterator<String> keys = properties.stringPropertyNames().iterator();
    return new Iterator<Map.Entry<String, String>>() {
      @Override
      public boolean hasNext() {
        return keys.hasNext();
      }

      @Override
      public Map.Entry<String, String> next() {
        String key = keys.next();
        return new AbstractMap.SimpleEntry<>(key, properties.getProperty(key));
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValorPropertiesConf that = (ValorPropertiesConf) o;
    return Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties);
  }
}
