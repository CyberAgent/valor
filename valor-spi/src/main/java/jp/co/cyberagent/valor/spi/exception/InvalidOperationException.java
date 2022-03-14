package jp.co.cyberagent.valor.spi.exception;

@SuppressWarnings("serial")
public class InvalidOperationException extends ValorException {
  public InvalidOperationException(String msg) {
    super(msg);
  }

  public InvalidOperationException(Exception e) {
    super(e);
  }

  public InvalidOperationException(String msg, Exception e) {
    super(msg, e);
  }
}
