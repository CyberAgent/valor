package jp.co.cyberagent.valor.sdk.serde;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import jp.co.cyberagent.valor.sdk.plan.visitor.AttributeCollectVisitor;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanImpl;

public class OneToOneQuerySerializer implements QuerySerializer {

  private FieldComparator comparator;

  @Override
  public List<StorageScan> serailize(
      Collection<String> attributes, Collection<PrimitivePredicate> conjunction,
      List<FieldLayout> layouts) throws ValorException {
    attributes = new HashSet<>(attributes);
    attributes.addAll(new AttributeCollectVisitor().walk(conjunction));

    Map<String, FieldComparator> filter = new HashMap<>();
    int maxRequiredFieldIndex = 0;
    for (int i = 0; i < layouts.size(); i++) {
      FieldLayout layout = layouts.get(i);
      comparator = new FieldComparator();
      for (Segment f : layout.formatters()) {
        if (maxRequiredFieldIndex < i) {
          if (attributes.stream().anyMatch(f::containsAttribute)) {
            maxRequiredFieldIndex = i;
          }
        }
        f.accept(this, conjunction);
      }
      filter.put(layout.fieldName(), comparator);
    }
    List<String> fields = IntStream.rangeClosed(0, maxRequiredFieldIndex)
        .mapToObj(layouts::get).map(FieldLayout::fieldName).collect(Collectors.toList());
    return Collections.singletonList(new StorageScanImpl(fields, filter));
  }

  @Override
  public void write(String type, FilterSegment fragment) {
    comparator.append(fragment);
  }
}
