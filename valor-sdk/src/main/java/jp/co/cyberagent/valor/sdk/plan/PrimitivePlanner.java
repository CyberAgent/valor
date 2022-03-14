package jp.co.cyberagent.valor.sdk.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import jp.co.cyberagent.valor.sdk.plan.visitor.AttributeCollectVisitor;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;

public abstract class PrimitivePlanner implements Planner<SimplePlan> {

  @Override
  public SimplePlan plan(List<ProjectionItem> items, PredicativeExpression condition,
                         Collection<Schema> schemas) throws ValorException {
    Collection<SimplePlan> plans = enumerateAllPlans(items, condition, schemas);
    double cost = Double.MAX_VALUE;
    SimplePlan selected = null;
    for (SimplePlan plan : plans) {
      double c = evaluate(plan);
      if (c <= cost) {
        cost = c;
        selected = plan;
      }
    }
    return selected;
  }

  @Override
  public Collection<SimplePlan> enumerateAllPlans(
      List<ProjectionItem> items, PredicativeExpression condition, Collection<Schema> schemas)
      throws ValorException {
    final AttributeCollectVisitor attrCollector = new AttributeCollectVisitor();
    items.forEach(attrCollector::walk);
    final Collection<String> attrs = attrCollector.getAttrributes();
    Collection<SimplePlan> plans = new ArrayList<>();
    for (Schema s : schemas) {
      SchemaScan ss = s.buildScan(attrs, condition);
      if (ss != null) {
        plans.add(new SimplePlan(items, ss));
      }
    }
    return plans;
  }

  @Override
  public abstract double evaluate(SimplePlan plan);

}
