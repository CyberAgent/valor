package jp.co.cyberagent.valor.sdk.plan;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import jp.co.cyberagent.valor.spi.util.TupleUtils;

public class HashJoinPlan implements QueryPlan {

  private final List<ProjectionItem> items;
  private final SchemaScan left;
  private final SchemaScan right;
  private final Expression condition;
  private final String[] joinKeys;

  public HashJoinPlan(List<ProjectionItem> items, SchemaScan largeScan, SchemaScan smallScan,
                      Expression condition, Collection<String> joinKeys) {
    if (!largeScan.getRelation().equals(smallScan.getRelation())) {
      throw new ValorRuntimeException("unexpected join of different relations: "
          + largeScan.getRelation() + " and " + smallScan.getRelation());
    }
    this.items = items;
    this.left = largeScan;
    this.right = smallScan;
    this.condition = condition;
    this.joinKeys = joinKeys.toArray(new String[joinKeys.size()]);
  }

  @Override
  public QueryRunner buildRunner(ValorConnection conn) throws ValorException {
    final Map<HashKey, Tuple> hashTable = new HashMap<>();
    try (StorageConnection sc = conn.createConnection(left.getSchema());
         SchemaScanner scanner = left.getSchema().getScanner(left, sc)) {
      Tuple t = null;
      while ((t = scanner.next()) != null) {
        hashTable.put(new HashKey(t), t);
      }
    } catch (IOException e) {
      throw new ValorException(e);
    }

    final StorageConnection sc = conn.createConnection(right.getSchema());
    final SchemaScanner scanner = right.getSchema().getScanner(right, sc);
    return new QueryRunner() {
      @Override
      public List<ProjectionItem> getItems() {
        return items;
      }

      @Override
      public Relation getRelation() {
        return left.getRelation();
      }

      @Override
      public Tuple next() throws ValorException {
        Tuple rt = null;
        try {
          while ((rt = scanner.next()) != null) {
            Tuple lt = hashTable.get(new HashKey(rt));
            if (lt != null) {
              return TupleUtils.deepMerge(lt, rt);
            }
          }
        } catch (IOException e) {
          throw new ValorException(e);
        }
        return null;
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
    throw new UnsupportedOperationException("count is not supported in " + getClass().getName());
  }

  class HashKey {
    private Object[] keys = new Object[joinKeys.length];

    public HashKey(Tuple t) {
      for (int i = 0; i < joinKeys.length; i++) {
        keys[i] = t.getAttribute(joinKeys[i]);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof HashKey)) {
        return false;
      }
      HashKey hashKey = (HashKey) o;
      return Arrays.equals(keys, hashKey.keys);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(keys);
    }
  }

}
