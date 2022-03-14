package jp.co.cyberagent.valor.spi.exception;

import jp.co.cyberagent.valor.spi.util.ByteUtils;

/**
 * encounter unexpected byte array
 */
@SuppressWarnings("serial")
public class MismatchByteArrayException extends SerdeException {

  public MismatchByteArrayException(String msg) {
    super(msg);
  }

  public MismatchByteArrayException(String msg, Exception e) {
    super(msg, e);
  }

  public MismatchByteArrayException(byte[] expected, byte[] actual) {
    super(String.format("%s is expected as prefix but %s received",
        ByteUtils.toStringBinary(expected), ByteUtils.toStringBinary(actual)));
  }
}
