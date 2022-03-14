package jp.co.cyberagent.valor.spi.plan;

import java.io.Closeable;
import java.util.List;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.ImmutableAttribute;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;

public interface TupleScanner extends Closeable {

  @Deprecated
  default List<Relation.Attribute> getAttribute() {
    return getItems().stream()
        .map(i -> ImmutableAttribute.of(i.getAlias(), false, i.getValue().getType()))
        .collect(Collectors.toList());
  }

  List<ProjectionItem> getItems();

  Tuple next() throws ValorException;

  Relation getRelation();
  
}
