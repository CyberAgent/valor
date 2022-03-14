package jp.co.cyberagent.valor.spi.serde;

import java.util.Collection;
import java.util.List;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.storage.StorageScan;

public abstract class QuerySerializerWrapper implements QuerySerializer {

  protected final QuerySerializer wrapped;

  protected QuerySerializerWrapper(QuerySerializer serializer) {
    this.wrapped = serializer;
  }

  @Override
  public List<StorageScan> serailize(
      Collection<String> attribute, Collection<PrimitivePredicate> conjunction,
      List<FieldLayout> layouts) {
    throw new UnsupportedOperationException();
  }
}
