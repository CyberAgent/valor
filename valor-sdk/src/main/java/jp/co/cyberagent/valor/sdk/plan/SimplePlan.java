package jp.co.cyberagent.valor.sdk.plan;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.QueryPlan;
import jp.co.cyberagent.valor.spi.plan.QueryRunner;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScanner;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimplePlan implements QueryPlan {

  private static final Logger LOG = LoggerFactory.getLogger(SimplePlan.class);

  protected final List<ProjectionItem> items;
  protected final SchemaScan scan;

  public SimplePlan(List<ProjectionItem> items, SchemaScan scan) {
    this.items = items;
    this.scan = scan;
  }

  public SchemaScan getScan() {
    return scan;
  }

  @Override
  public QueryRunner buildRunner(ValorConnection conn) throws ValorException {
    final StorageConnection sc = conn.createConnection(scan.getSchema());
    final SchemaScanner scanner = scan.getSchema().getScanner(scan, sc);
    final Relation relation = scan.getRelation();

    return new QueryRunner() {
      @Override
      public List<ProjectionItem> getItems() {
        return items;
      }

      @Override
      public Relation getRelation() {
        return relation;
      }

      @Override
      public Tuple next() throws ValorException {
        try {
          return scanner.next();
        } catch (IOException e) {
          throw new ValorException(e);
        }
      }

      @Override
      public void close() throws IOException {
        try {
          scanner.close();
        } finally {
          sc.close();
        }
      }
    };
  }

  @Override
  public Optional<Long> count(ValorConnection conn) throws ValorException {
    if (!isCountable()) {
      return Optional.empty();
    }
    try (StorageConnection sc = conn.createConnection(scan.getSchema())) {
      return sc.count(scan);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  private boolean isCountable() {
    PredicativeExpression predicate = scan.getFilter();
    OrOperator dnf = predicate.getDnf();
    if (dnf.getOperands().size() != 1) {
      LOG.info("or operator is not supported by count()");
      return false;
    }
    List<PrimitivePredicate> conjunction =
        ((AndOperator) dnf.getOperands().get(0)).getOperands().stream()
            .map(PrimitivePredicate.class::cast).collect(Collectors.toList());
    return scan.getFragments().stream().allMatch(s -> isCountable(s, conjunction));
  }

  private boolean isCountable(StorageScan ss, List<PrimitivePredicate> conjunction) {
    Set<PrimitivePredicate> predicates = new HashSet<>(conjunction);
    for (String field : ss.getFields()) {
      FieldComparator fieldComparator = ss.getFieldComparator(field);
      if (fieldComparator != null) {
        List<PredicativeExpression> pushedDownPredicates = fieldComparator.getEmbeddedPredicates();
        for (PredicativeExpression pushedDownPredicate : pushedDownPredicates) {
          if (pushedDownPredicate instanceof AndOperator) {
            predicates.removeAll(((AndOperator) pushedDownPredicate).getOperands());
          } else {
            predicates.remove(pushedDownPredicate);
          }
        }
      }
    }
    if (!predicates.isEmpty()) {
      LOG.warn("{} is not pushed down",
          predicates.stream().map(PrimitivePredicate::toString).collect(Collectors.joining(", ")));
      return false;
    }
    return true;
  }

}
