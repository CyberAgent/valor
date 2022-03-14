package jp.co.cyberagent.valor.spi.optimize;

import jp.co.cyberagent.valor.spi.plan.ScanPlan;

public class EvaluatedPlan {

  private final ScanPlan plan;
  private final double cost;

  public EvaluatedPlan(ScanPlan plan, double cost) {
    this.plan = plan;
    this.cost = cost;
  }

  public ScanPlan getPlan() {
    return plan;
  }

  public double getCost() {
    return cost;
  }

}
