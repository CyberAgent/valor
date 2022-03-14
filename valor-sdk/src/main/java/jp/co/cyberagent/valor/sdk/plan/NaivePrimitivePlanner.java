package jp.co.cyberagent.valor.sdk.plan;

import java.util.List;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.IsNullOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.visitor.LogicalPlanVisitorBase;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.PlannerFactory;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.NotOperator;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.UnaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.StorageScan;

public class NaivePrimitivePlanner extends PrimitivePlanner {

  public static final String NAME = "naive";

  @Override
  public double evaluate(SimplePlan plan) {
    SimplePlan sp = (SimplePlan)plan;
    SchemaScan scan = sp.getScan();
    List<StorageScan> storageScans = scan.getFragments();
    return storageScans.stream().mapToDouble(this::evaluate).sum();
  }

  private double evaluate(StorageScan storageScan) {
    // TODO consider all key fields
    double cost = 1.0;
    FieldComparator comparator = storageScan.getFieldComparator(storageScan.getFields().get(0));
    if (comparator == null) {
      return 1.0;
    }
    List<PredicativeExpression> predicates = comparator.getEmbeddedPredicates();
    for (int i = 0; i < predicates.size(); i++) {
      double filterEffect = new PredicateEvaluator().walk(predicates.get(i));
      if (filterEffect == 0) {
        return cost;
      }
      cost = cost / filterEffect;
    }
    return cost;
  }

  static class PredicateEvaluator extends LogicalPlanVisitorBase<Double> {

    static final double ASSUMED_CARDINALITY = 10000;

    protected boolean visitBinaryPrimitivePredicate(BinaryPrimitivePredicate e) {
      if (e instanceof EqualOperator) {
        this.result = ASSUMED_CARDINALITY;
      } else if (
          e instanceof LessthanOperator || e instanceof LessthanorequalOperator
              || e instanceof GreaterthanOperator || e instanceof GreaterthanorequalOperator
      ) {
        this.result = ASSUMED_CARDINALITY / 2;
      } else {
        this.result = 0.0;
      }
      return false;
    }

    protected boolean visitUnaryPrimitivePredicate(UnaryPrimitivePredicate e) {
      if (e instanceof IsNullOperator) {
        this.result = ASSUMED_CARDINALITY;
      } else {
        this.result = 0.0;
      }
      return false;
    }

    protected boolean visitNotOperator(NotOperator e) {
      this.result = 0.0;
      return false;
    }

    protected void leaveAndOperator(AndOperator e) {
      this.result = childResult.peek().stream().max(Double::compareTo).orElse(0.0);
    }

    protected void leaveOrOperator(OrOperator e) {
      this.result = childResult.peek().stream().min(Double::compareTo).orElse(0.0);
    }

  }


  public static class Factory implements PlannerFactory {

    @Override
    public Planner create(ValorConf config) {
      return new NaivePrimitivePlanner();
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Planner> getProvidedClass() {
      return NaivePrimitivePlanner.class;
    }
  }
}
