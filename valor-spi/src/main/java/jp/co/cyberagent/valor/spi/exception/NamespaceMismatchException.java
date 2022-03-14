package jp.co.cyberagent.valor.spi.exception;

public class NamespaceMismatchException extends ValorRuntimeException {

  public NamespaceMismatchException(String expectedNs, String acutalNs) {
    super(String.format("namespace mismatch: expected %s, but %s", expectedNs, acutalNs));
  }
}
