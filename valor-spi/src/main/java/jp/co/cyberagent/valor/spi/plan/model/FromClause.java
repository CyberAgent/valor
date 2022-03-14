package jp.co.cyberagent.valor.spi.plan.model;

import java.util.List;
import jp.co.cyberagent.valor.spi.relation.Relation;

public interface FromClause extends LogicalPlanNode {

  List<Relation.Attribute> getAttributes();

}
