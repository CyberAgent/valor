package jp.co.cyberagent.valor.sdk.formatter;

import jp.co.cyberagent.valor.spi.schema.Formatter;

public abstract class SaltFormatter extends Formatter {

  public abstract int getSaltSize();
}
