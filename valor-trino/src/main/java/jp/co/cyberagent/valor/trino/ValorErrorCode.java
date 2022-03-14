package jp.co.cyberagent.valor.trino;

import static io.trino.spi.ErrorType.EXTERNAL;

import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;


/**
 *
 */
public enum ValorErrorCode implements ErrorCodeSupplier {

  VALOR_ERROR(0, EXTERNAL);

  private final ErrorCode errorCode;

  ValorErrorCode(int code, ErrorType type) {
    errorCode = new ErrorCode(code + 0x7F00_0000, name(), type);
  }

  @Override
  public ErrorCode toErrorCode() {
    return errorCode;
  }
}
