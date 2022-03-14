package jp.co.cyberagent.valor.sdk.plan;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.plan.QueryPlan;
import jp.co.cyberagent.valor.spi.plan.QueryRunner;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScanner;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.util.Closer;
import jp.co.cyberagent.valor.spi.util.TupleUtils;

// experimental
public class MergeJoinPlan implements QueryPlan {

  private final List<ProjectionItem> items;
  private final SchemaScan largeScan;
  private final SchemaScan smallScan;
  private final Expression condition;
  private final String[] joinKeys;

  public MergeJoinPlan(List<ProjectionItem> items, SchemaScan largeScan, SchemaScan smallScan,
                       Expression condition, Collection<String> joinKeys) {
    if (!largeScan.getRelation().equals(smallScan.getRelation())) {
      throw new ValorRuntimeException("unexpected join of different relations: "
          + largeScan.getRelation() + " and " + smallScan.getRelation());
    }
    this.items = items;
    this.largeScan = largeScan;
    this.smallScan = smallScan;
    this.condition = condition;
    this.joinKeys = joinKeys.toArray(new String[joinKeys.size()]);
  }

  @Override
  public QueryRunner buildRunner(ValorConnection conn) throws ValorException {
    final StorageConnection sc = conn.createConnection(smallScan.getSchema());
    final StorageConnection lc = conn.createConnection(largeScan.getSchema());
    final SchemaScanner smallScanner = smallScan.getSchema().getScanner(smallScan, sc);
    final SchemaScanner largeScanner = largeScan.getSchema().getScanner(largeScan, lc);

    return new MergeJoinRunner(smallScanner, largeScanner, lc, sc, joinKeys);
  }

  @Override
  public Optional<Long> count(ValorConnection conn) throws ValorException {
    throw new UnsupportedOperationException("count is not supported in " + getClass().getName());
  }

  private class MergeJoinRunner implements QueryRunner {

    private final SchemaScanner smallScanner;
    private final SchemaScanner largeScanner;
    private final StorageConnection lc;
    private final StorageConnection sc;
    private final String[] joinKeys;

    private Tuple st;

    public MergeJoinRunner(SchemaScanner smallScanner, SchemaScanner largeScanner,
                           StorageConnection lc, StorageConnection sc,
                           String[] joinKeys) throws ValorException {
      this.smallScanner = smallScanner;
      this.largeScanner = largeScanner;
      this.lc = lc;
      this.sc = sc;
      this.joinKeys = joinKeys;
      try {
        this.st = smallScanner.next();
      } catch (IOException e) {
        throw new ValorException(e);
      }
    }

    @Override
    public List<ProjectionItem> getItems() {
      return items;
    }

    @Override
    public Relation getRelation() {
      return largeScan.getRelation();
    }

    @Override
    public Tuple next() throws ValorException {
      if (st == null) {
        return null;
      }
      try {
        Tuple lt = largeScanner.next();
        if (lt == null) {
          return null;
        }
        int c = compareKeys(st, lt);
        if (c == 0) {
          return TupleUtils.deepMerge(st, lt);
        }
        if (c < 0) {
          return proceedSmall(lt);
        } else {
          return next();
        }
      } catch (IOException e) {
        throw new ValorException(e);
      }
    }

    private Tuple proceedSmall(Tuple lt) throws ValorException, IOException {
      st = smallScanner.next();
      if (st == null) {
        return null;
      }
      int c = compareKeys(st, lt);
      if (c == 0) {
        return TupleUtils.deepMerge(st, lt);
      }
      if (c < 0) {
        return proceedSmall(lt);
      } else {
        return next();
      }
    }


    private int compareKeys(Tuple left, Tuple right) {
      for (String k : joinKeys) {
        // assume key is not null
        Comparable lk = (Comparable) left.getAttribute(k);
        Comparable rk = (Comparable) right.getAttribute(k);
        int c = lk.compareTo(rk);
        if (c != 0) {
          return c;
        }
      }
      return 0;
    }

    @Override
    public void close() throws IOException {
      Closer closer = new Closer();
      closer.close(largeScanner);
      closer.close(smallScanner);
      closer.close(lc);
      closer.close(sc);
      closer.throwIfFailed();
    }
  }
}
