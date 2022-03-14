package jp.co.cyberagent.valor.spi.plan;

import java.util.Optional;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;

public interface ScanPlan {

  @Deprecated
  QueryRunner buildRunner(ValorConnection conn) throws ValorException;

  default TupleScanner scanner(ValorConnection conn) throws ValorException {
    return buildRunner(conn);
  }

  Optional<Long> count(ValorConnection conn) throws ValorException;

}
