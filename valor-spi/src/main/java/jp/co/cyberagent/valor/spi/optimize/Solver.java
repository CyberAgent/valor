package jp.co.cyberagent.valor.spi.optimize;

import java.util.Collection;
import java.util.Map;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;

public interface Solver {

  String getName();

  Map<String, ScanPlan> solve(Map<String, Collection<EvaluatedPlan>> plansWithCosts);
}
