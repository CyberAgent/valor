package jp.co.cyberagent.valor.spi.optimize;

import java.util.List;

public interface Optimizer {

  Enumerator getEnumerator();

  List<Solver> getSolvers();
}
