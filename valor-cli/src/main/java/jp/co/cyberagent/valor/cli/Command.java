package jp.co.cyberagent.valor.cli;

import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.ValorException;

public interface Command {

  int execute(Map<String, String> conf) throws ValorException;

}
