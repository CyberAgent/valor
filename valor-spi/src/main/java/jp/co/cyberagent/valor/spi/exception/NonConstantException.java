package jp.co.cyberagent.valor.spi.exception;

@SuppressWarnings("serial")
public class NonConstantException extends RuntimeException {

  public NonConstantException(String msg) {
    super(msg);
  }
}
