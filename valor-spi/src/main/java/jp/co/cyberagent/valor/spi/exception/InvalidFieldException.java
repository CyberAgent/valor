package jp.co.cyberagent.valor.spi.exception;

/**
 *
 */
public class InvalidFieldException extends SerdeException {
  public InvalidFieldException(String field, String storageType) {
    super(String.format("%s is not supported in %s", field, storageType));
  }
}
