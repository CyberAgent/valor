package jp.co.cyberagent.valor.spi.plan;

import java.util.Collection;
import java.util.List;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.schema.Schema;

public interface Planner<P extends ScanPlan> {

  @Deprecated
  default P plan(RelationScan scan, SchemaRepository repository)
      throws ValorException {
    final Collection<Schema> schemas = scan.getFrom().listSchemas(repository);
    final List<ProjectionItem> items = scan.getProjection().getItems();
    final PredicativeExpression condition = scan.getCondition();
    return plan(items, condition, schemas);
  }

  P plan(List<ProjectionItem> items, PredicativeExpression condition, Collection<Schema> schemas)
      throws ValorException;

  @Deprecated
  default Collection<P> enumerateAllPlans(RelationScan scan, SchemaRepository repository)
      throws ValorException {
    final Collection<Schema> schemas = scan.getFrom().listSchemas(repository);
    final List<ProjectionItem> items = scan.getProjection().getItems();
    final PredicativeExpression condition = scan.getCondition();
    return enumerateAllPlans(items, condition, schemas);
  }

  Collection<P> enumerateAllPlans(
      List<ProjectionItem> items, PredicativeExpression condition, Collection<Schema> schemas)
      throws ValorException;

  /**
   *
   * @param plan a query plan to be evaluated
   * @return cost for executing the plan. The cost ranges from 0 to 1 and 1 indicates full scan.
   */
  double evaluate(P plan);

}
