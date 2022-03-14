package jp.co.cyberagent.valor.spi;

import java.util.Map;
import jp.co.cyberagent.valor.spi.conf.ValorConfParam;

/**
 *
 */
public interface ValorConf extends Iterable<Map.Entry<String, String>> {

  ValorConfParam IGNORE_INVALID_RECORD = new ValorConfParam("valor.client.ignoreInvalidRecord",
      "false");

  void set(String name, String value);

  String get(String name);

  boolean containsKey(String name);
}
