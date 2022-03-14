package jp.co.cyberagent.valor.spi.serde;

import java.util.List;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.storage.Record;

public abstract class TupleSerializerWrapper implements TupleSerializer {

  protected final TupleSerializer wrapped;

  protected TupleSerializerWrapper(TupleSerializer serializer) {
    this.wrapped = serializer;
  }

  public TupleSerializer getWrapped() {
    return wrapped;
  }

  @Override
  public List<Record> serailize(Tuple tuple, List<FieldLayout> layouts) throws ValorException {
    throw new UnsupportedOperationException();
  }
}
