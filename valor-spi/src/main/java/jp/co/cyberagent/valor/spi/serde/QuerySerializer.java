package jp.co.cyberagent.valor.spi.serde;

import java.util.Collection;
import java.util.List;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.storage.StorageScan;

public interface QuerySerializer {

  List<StorageScan> serailize(
      Collection<String> attributes, Collection<PrimitivePredicate> conjunction,
      List<FieldLayout> layouts) throws ValorException;

  void write(String type, FilterSegment fragment);
}
