package jp.co.cyberagent.valor.hbase.exception;

import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class InvalidRowException extends ValorRuntimeException {
  public InvalidRowException(byte[] rowkey, String messgae) {
    super(String.format("row %s is invalid: %s", ByteUtils.toString(rowkey), messgae));
  }
}
