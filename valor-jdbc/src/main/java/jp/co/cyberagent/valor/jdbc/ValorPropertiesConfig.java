package jp.co.cyberagent.valor.jdbc;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import jp.co.cyberagent.valor.spi.ValorConf;

public class ValorPropertiesConfig implements ValorConf {

  private final Properties properties;

  public ValorPropertiesConfig(Properties properties) {
    this.properties = properties;
  }


  @Override
  public void set(String name, String value) {
    properties.setProperty(name, value);
  }

  @Override
  public String get(String name) {
    Object v = properties.getProperty(name);
    return v == null ? null : (String) v;
  }

  @Override
  public boolean containsKey(String name) {
    return properties.containsKey(name);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    Enumeration<?> e = properties.propertyNames();
    return new Iterator<Map.Entry<String, String>>() {
      @Override
      public boolean hasNext() {
        return e.hasMoreElements();
      }

      @Override
      public Map.Entry<String, String> next() {
        String key = (String) e.nextElement();
        String v = properties.getProperty(key);
        return new Map.Entry<String, String>() {
          @Override
          public String getKey() {
            return key;
          }

          @Override
          public String getValue() {
            return v;
          }

          @Override
          public String setValue(String value) {
            throw new UnsupportedOperationException();
          }
        };
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
    ValorPropertiesConfig entries = (ValorPropertiesConfig) o;
    return Objects.equals(properties, entries.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties);
  }

}
