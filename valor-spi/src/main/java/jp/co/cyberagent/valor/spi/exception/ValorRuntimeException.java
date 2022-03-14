package jp.co.cyberagent.valor.spi.exception;

public class ValorRuntimeException extends RuntimeException {

  public ValorRuntimeException(String message) {
    super(message);
  }

  public ValorRuntimeException(Throwable cause) {
    super(cause);
  }

  public ValorRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }
}
