package jp.co.cyberagent.valor.spi.exception;

@SuppressWarnings("serial")
public class UnsupportedSchemaException extends InvalidOperationException {

  public UnsupportedSchemaException(String msg) {
    super(msg);
  }
}
