package jp.co.cyberagent.valor.spi.schema.scanner;

import java.util.Comparator;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.storage.StorageScan;

/**
 *
 */
public class SchemaScan {

  private Relation relation;

  private Schema schema;

  private PredicativeExpression filter;

  // sorted by start key
  // elements with the same start key should be merged and therefore multiple elements where
  // compare(o1,o2) = 0 should not be exist.
  private NavigableSet<StorageScan> fragments;

  private Comparator<StorageScan> overlapComparator;

  private ValorConf conf;

  public SchemaScan(Relation relation, Schema schema, PredicativeExpression filter) {
    this(relation, schema, filter, null);
  }

  public SchemaScan(
      Relation relation, Schema schema, PredicativeExpression filter, ValorConf conf) {
    this.relation = relation;
    this.schema = schema;
    this.filter = filter;
    this.conf = conf == null ? schema.getConf() : conf;
    List<String> fields = schema.getFields();
    this.fragments = new TreeSet<>(new StorageScan.StorageScanFragmentComparator(fields));
    this.overlapComparator = new StorageScan.StorageScanFragmentComparator(fields,
        StorageScan.EXTRACT_STOP, StorageScan.EXTRACT_START);
  }

  public void add(StorageScan fragment) {
    StorageScan floor = fragments.floor(fragment);
    if (floor == null) {
      List<StorageScan> tobeMerged =
          fragments.stream().filter(f -> overlapComparator.compare(fragment, f) > 0)
              .collect(Collectors.toList());
      for (StorageScan f : tobeMerged) {
        fragments.remove(f);
        fragment.mergeByOr(f);
      }
      fragments.add(fragment);
      return;
    }
    int overlap = overlapComparator.compare(floor, fragment);
    if (overlap < 0) {
      fragments.add(fragment);
      return;
    }
    fragments.remove(floor);
    floor.mergeByOr(fragment);
    fragments.add(floor);
  }

  public Relation getRelation() {
    return relation;
  }

  public Schema getSchema() {
    return schema;
  }

  public PredicativeExpression getFilter() {
    return filter;
  }

  public List<StorageScan> getFragments() {
    return fragments.stream().collect(Collectors.toList());
  }

  public ValorConf getConf() {
    return conf;
  }

  public void setConf(ValorConf conf) {
    this.conf = conf;
  }
}
