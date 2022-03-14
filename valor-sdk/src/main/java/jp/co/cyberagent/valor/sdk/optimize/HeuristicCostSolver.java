package jp.co.cyberagent.valor.sdk.optimize;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.optimize.EvaluatedPlan;
import jp.co.cyberagent.valor.spi.optimize.Solver;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;

public class HeuristicCostSolver implements Solver {

  @Override
  public Map<String, ScanPlan> solve(Map<String, Collection<EvaluatedPlan>> plansWithCost) {
    return plansWithCost.entrySet().stream().collect(Collectors.toMap(
        e -> e.getKey(),
        e -> chooseMinCostPlans(e.getValue())
    ));
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

  private static ScanPlan chooseMinCostPlans(Collection<EvaluatedPlan> plans) {
    return plans.stream().min((o1, o2) -> {
      double c = o1.getCost() - o2.getCost();
      if (c == 0) {
        return 0;
      } else {
        return c < 0 ? -1 : 1;
      }
    }).get().getPlan();
  }
}
