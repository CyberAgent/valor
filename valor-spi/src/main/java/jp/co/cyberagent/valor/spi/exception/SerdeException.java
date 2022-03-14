package jp.co.cyberagent.valor.spi.exception;

/**
 * thrown when conversion between tuples and key-values failed
 */
@SuppressWarnings("serial")
public class SerdeException extends ValorRuntimeException {

  public SerdeException(String msg) {
    super(msg);
  }

  public SerdeException(Throwable e) {
    super(e);
  }

  public SerdeException(String msg, Throwable e) {
    super(msg, e);
  }
}
