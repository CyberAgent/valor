package jp.co.cyberagent.valor.hbase.optimize;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.optimize.HeuristicCostSolver;
import jp.co.cyberagent.valor.sdk.optimize.HeuristicSpaceSolver;
import jp.co.cyberagent.valor.spi.optimize.Enumerator;
import jp.co.cyberagent.valor.spi.optimize.Optimizer;
import jp.co.cyberagent.valor.spi.optimize.OptimizerFactory;
import jp.co.cyberagent.valor.spi.optimize.Solver;

public class HBaseOptimizer implements Optimizer {

  private static final String OPTIMIZER_TYPE = "HBaseOptimizer";

  @Override
  public Enumerator getEnumerator() {
    return new HBaseEnumerator();
  }

  @Override
  public List<Solver> getSolvers() {
    List<Solver> solvers = new ArrayList<>();
    solvers.add(new HeuristicCostSolver());
    solvers.add(new HeuristicSpaceSolver());
    return solvers;
  }

  public static class Factory implements OptimizerFactory {

    @Override
    public Optimizer create(Map config) {
      return new HBaseOptimizer();
    }

    @Override
    public String getName() {
      return OPTIMIZER_TYPE;
    }

    @Override
    public Class<? extends Optimizer> getProvidedClass() {
      return HBaseOptimizer.class;
    }
  }
}
