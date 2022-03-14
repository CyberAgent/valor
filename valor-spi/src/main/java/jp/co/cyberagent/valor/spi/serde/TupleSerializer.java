package jp.co.cyberagent.valor.spi.serde;

import java.util.List;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.storage.Record;

public interface TupleSerializer {

  List<Record> serailize(Tuple tuple, List<FieldLayout> layouts) throws ValorException;

  void write(String type, byte[]... values);
}
