package jp.co.cyberagent.valor.spi.exception;

import jp.co.cyberagent.valor.spi.storage.StorageScan;

/**
 *
 */
public class IllegalScanException extends ValorException {
  public IllegalScanException(String message, StorageScan scan) {
    super(message + " in " + scan.toString());
  }
}
