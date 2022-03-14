package jp.co.cyberagent.valor.spi.conf;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import jp.co.cyberagent.valor.spi.ValorConf;

public class ValorConfImpl implements ValorConf {

  private Map<String, String> values = new HashMap<>();

  public ValorConfImpl() {
  }

  public ValorConfImpl(Map<String, String> conf) {
    this.values = conf;
  }

  @Override
  public void set(String name, String value) {
    values.put(name, value);
  }

  @Override
  public String get(String name) {
    return values.get(name);
  }

  @Override
  public boolean containsKey(String name) {
    return values.containsKey(name);
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return values.entrySet().iterator();
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(", ", "{", "}");
    values.forEach((k, v) -> joiner.add(String.format("%s: %s", k, v)));
    return joiner.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValorConfImpl otherConf = (ValorConfImpl) o;
    for (Map.Entry<String, String> entries : values.entrySet()) {
      String otherValue = otherConf.get(entries.getKey());
      if (entries.getValue() == null && otherValue != null) {
        return false;
      }
      if (!entries.getValue().equals(otherValue)) {
        return false;
      }
    }
    return values.size() == otherConf.values.size();
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }
}
