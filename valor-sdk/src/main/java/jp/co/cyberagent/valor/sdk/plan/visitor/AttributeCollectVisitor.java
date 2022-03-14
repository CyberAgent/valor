package jp.co.cyberagent.valor.sdk.plan.visitor;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;

public class AttributeCollectVisitor implements LogicalPlanVisitor {

  private Set<String> attrributes = new HashSet<>();

  public Set<String> getAttrributes() {
    return attrributes;
  }


  public Set<String> walk(LogicalPlanNode exp) {
    exp.accept(this);
    return attrributes;
  }

  public Set<String> walk(Collection<? extends LogicalPlanNode> exp) {
    exp.forEach(e -> e.accept(this));
    return attrributes;
  }

  public void clear() {
    attrributes.clear();
  }

  @Override
  public boolean visit(LogicalPlanNode e) {
    if (e instanceof AttributeNameExpression) {
      attrributes.add(((AttributeNameExpression) e).getName());
    }
    return true;
  }

  @Override
  public void leave(LogicalPlanNode node) {
  }
}
