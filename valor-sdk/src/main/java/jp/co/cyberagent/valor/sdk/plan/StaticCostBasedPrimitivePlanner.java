package jp.co.cyberagent.valor.sdk.plan;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.optimize.DataStats;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.PlannerFactory;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Schema;

public class StaticCostBasedPrimitivePlanner extends PrimitivePlanner {

  public static final String NAME = "staticCostBased";

  public static final String STATS_FILE_PATH = "valor.plan.staticCostBased.stats.file";

  private final Map<String, DataStats> dataStats;

  public StaticCostBasedPrimitivePlanner(Map<String, DataStats> dataStats) {
    this.dataStats = dataStats;
  }

  @Override
  public double evaluate(SimplePlan plan) {
    Schema schema = plan.getScan().getSchema();
    PredicativeExpression condition = plan.getScan().getFilter();

    // row key: A-B-C-D-E, query eq predicate: A = ? AND B = ? AND C = ?
    // lookup cost: 1 / (A.cardinality * 1 + 0.1 * B.cardinality + 0.01 * C.cardinality)
    OrOperator dnf = condition.getDnf();
    String relationId = plan.getScan().getSchema().getRelationId();
    DataStats relStats = dataStats.get(relationId);
    double cost = 0;
    for (PredicativeExpression conjunction : dnf) {
      double c = calculateCost((AndOperator) conjunction, schema, relStats);
      if (c > cost) {
        cost = c;
      }
    }
    return cost;
  }

  private double calculateCost(AndOperator conjunction, Schema schema, DataStats stats) {
    List<FilterEffect> filterEffects = conjunction.getOperands().stream()
        .map(p -> new FilterEffect((PrimitivePredicate) p))
        .filter(p -> p.attr != null)
        .collect(Collectors.toList());

    double estimatedFilterValue = 0.0;
    int segmentIndex = 0;
    // TODO determine schema from storage
    List<String> fields = schema.getFields();
    for (String field : fields) {
      FieldLayout layout = schema.getLayout(field);
      List<Formatter> formatters = layout.formatters().stream()
          .map(s -> s.getFormatter())
          .filter(f -> !(f instanceof ConstantFormatter))
          .collect(Collectors.toList());
      for (int i = 0; i < formatters.size(); i++) {
        Formatter formatter = formatters.get(i).getFormatter();
        Optional<FilterEffect> effect = findRelatedAttribute(filterEffects, formatter);
        if (effect.isPresent()) {
          FilterEffect e = effect.get();
          estimatedFilterValue
              += stats.getCardinality().get(e.attr) * e.effect * Math.pow(0.1, segmentIndex + 1);
        }
        segmentIndex++;
      }
    }
    return 1.0 / estimatedFilterValue;
  }

  // FIXME support formatter contains multiple attributes
  private Optional<FilterEffect> findRelatedAttribute(
      Collection<FilterEffect> candidates, Formatter formatter) {
    return candidates.stream().filter(a -> formatter.containsAttribute(a.attr)).findAny();
  }

  static class FilterEffect {
    public final String attr;
    public final double effect;

    public FilterEffect(PrimitivePredicate predicate) {
      if (predicate instanceof BinaryPrimitivePredicate) {
        this.attr = ((BinaryPrimitivePredicate) predicate).getAttributeIfUnaryPredicate();
        if (predicate instanceof EqualOperator) {
          this.effect = 1;
        } else if (predicate instanceof LessthanOperator
            || predicate instanceof LessthanorequalOperator
            || predicate instanceof GreaterthanOperator
            || predicate instanceof GreaterthanorequalOperator) {
          this.effect = 0.5;
        } else {
          this.effect = 0;
        }
      } else {
        this.attr = null;
        this.effect = 0;
      }
    }
  }


  public static class Factory implements PlannerFactory {

    static final TypeReference<Map<String, DataStats>> STATS_TYPE
        = new TypeReference<Map<String, DataStats>>() {};

    @Override
    public Planner create(ValorConf config) {
      String path = config.get(STATS_FILE_PATH);
      ObjectMapper om = new ObjectMapper();
      try {
        Map<String, DataStats> stats = om.readValue(new File(path), STATS_TYPE);
        return new StaticCostBasedPrimitivePlanner(stats);
      } catch (IOException e) {
        throw new ValorRuntimeException(e);
      }

    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Planner> getProvidedClass() {
      return StaticCostBasedPrimitivePlanner.class;
    }
  }
}
