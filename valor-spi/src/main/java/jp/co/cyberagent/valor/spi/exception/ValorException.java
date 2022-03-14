package jp.co.cyberagent.valor.spi.exception;

/**
 *
 */
public class ValorException extends Exception {

  public ValorException() {
    super();
  }

  public ValorException(String message) {
    super(message);
  }

  public ValorException(String message, Throwable cause) {
    super(message, cause);
  }

  public ValorException(Throwable cause) {
    super(cause);
  }
}
