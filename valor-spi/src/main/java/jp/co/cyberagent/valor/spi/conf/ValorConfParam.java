package jp.co.cyberagent.valor.spi.conf;

import jp.co.cyberagent.valor.spi.ValorConf;

public class ValorConfParam {

  public final String name;
  public final String defaultValue;

  public ValorConfParam(String name, String defaultValue) {
    this.name = name;
    this.defaultValue = defaultValue;
  }

  public String get(ValorConf conf) {
    String value = conf.get(name);
    return value == null ? defaultValue : value;
  }
}
