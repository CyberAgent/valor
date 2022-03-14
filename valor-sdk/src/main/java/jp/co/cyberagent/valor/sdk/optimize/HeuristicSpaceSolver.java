package jp.co.cyberagent.valor.sdk.optimize;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.plan.SimplePlan;
import jp.co.cyberagent.valor.spi.optimize.EvaluatedPlan;
import jp.co.cyberagent.valor.spi.optimize.Solver;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.util.Pair;

public class HeuristicSpaceSolver implements Solver {

  @Override
  public Map<String, ScanPlan> solve(Map<String, Collection<EvaluatedPlan>> plansWithCost) {
    // TODO consider join plan
    Map<SchemaDescriptor, Collection<Pair<String, ScanPlan>>> coveredQueries = new HashMap<>();
    for (Map.Entry<String, Collection<EvaluatedPlan>> e : plansWithCost.entrySet()) {
      String queryName = e.getKey();
      for (EvaluatedPlan ep : e.getValue()) {
        ScanPlan p = ep.getPlan();
        if (!(p instanceof SimplePlan)) {
          throw new UnsupportedOperationException("only simple plan is supported, but " + p);
        }
        SimplePlan sp = (SimplePlan) p;
        SchemaDescriptor schema = SchemaDescriptor.from(sp.getScan().getSchema());
        Collection<Pair<String, ScanPlan>> queries = coveredQueries.get(schema);
        Pair<String, ScanPlan> namedPlan = new Pair<>(queryName, p);
        if (queries != null) {
          queries.add(namedPlan);
        } else {
          queries = new HashSet<>();
          queries.add(namedPlan);
          coveredQueries.put(schema, queries);
        }
      }
    }

    Map<String, ScanPlan> plans = new HashMap<>();
    Set<String> queries = new HashSet<>(plansWithCost.keySet());
    while (!queries.isEmpty()) {
      SchemaDescriptor schema = findMostUsedSchema(coveredQueries);
      if (schema == null) {
        throw new IllegalArgumentException("unable to find appropriate schema from candidates");
      }
      Collection<Pair<String, ScanPlan>> queryNames = coveredQueries.get(schema);
      for (Pair<String, ScanPlan> qp: queryNames) {
        plans.put(qp.getFirst(), qp.getSecond());
        queries.remove(qp.getFirst());
      }
      coveredQueries = coveredQueries.entrySet().stream()
          .filter(e -> !e.getKey().equals(schema))
          .collect(Collectors.toMap(
              e -> e.getKey(),
              e -> e.getValue().stream()
                  .filter(q -> !queries.contains(q)).collect(Collectors.toSet())
          ));

    }
    return plans;
  }

  private SchemaDescriptor findMostUsedSchema(
      Map<SchemaDescriptor, Collection<Pair<String, ScanPlan>>> covered) {
    SchemaDescriptor schema = null;
    int c = 0;
    for (Map.Entry<SchemaDescriptor, Collection<Pair<String, ScanPlan>>> e : covered.entrySet()) {
      if (e.getValue().size() > c) {
        c = e.getValue().size();
        schema = e.getKey();
      }
    }
    return schema;
  }

  @Override
  public String getName() {
    return this.getClass().getName();
  }

}
