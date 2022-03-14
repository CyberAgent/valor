package jp.co.cyberagent.valor.spi.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.InvalidOperationException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;
import jp.co.cyberagent.valor.spi.util.Pair;

public abstract class SchemaHandlerBase implements SchemaHandler {

  protected Relation relation;

  protected Map<String, Object> conf;

  @Override
  public Map<String, Object> getConf() {
    return conf;
  }

  @Override
  public SchemaHandler init(Relation relation) {
    this.relation = relation;
    return this;
  }

  @Override
  public SchemaHandler configure(Map<String, Object> conf) {
    this.conf = conf;
    return this;
  }

  @Override
  public ScanPlan plan(ValorConnection conn,
                       Collection<Schema> schemas,
                       List<ProjectionItem> items,
                       PredicativeExpression condition,
                       Planner planner) throws ValorException {
    return planner.plan(items, condition, schemas);
  }

  @Override
  public TupleScanner scanner(ValorConnection conn,
                              Collection<Schema> schemas,
                              List<ProjectionItem> items,
                              PredicativeExpression condition,
                              Planner planner) throws ValorException {
    ScanPlan plan = planner.plan(items, condition, schemas);
    if (plan == null) {
      StringBuilder msg = new StringBuilder()
          .append("failed to build plan for 'SELECT ")
          .append(items)
          .append(" FROM ")
          .append(relation.getRelationId())
          .append(" WHERE ")
          .append(condition)
          .append("'");
      throw new InvalidOperationException(msg.toString());
    }
    return plan.buildRunner(conn);
  }

  @Override
  public void insert(ValorConnection conn, Collection<Schema> schemas, Collection<Tuple> tuples)
      throws ValorException, IOException {
    List<Pair<Schema, StorageMutation>> mutations = new ArrayList<>(schemas.size());
    for (Schema targetSchema : schemas) {
      mutations.add(new Pair<>(targetSchema, targetSchema.buildInsertMutation(tuples)));
    }
    // TODO make atomic
    for (Pair<Schema, StorageMutation> mutation : mutations) {
      conn.submit(mutation.getFirst(), mutation.getSecond());
    }
  }

  @Override
  public int delete(ValorConnection conn, Collection<Schema> schemas, TupleScanner scanner,
                    Integer limit) throws ValorException, IOException {
    int numDeletes = 0;
    Tuple deletion;
    int cnt = limit == null ? -1 : limit;
    while ((deletion = scanner.next()) != null && cnt != 0) {
      cnt--;
      // TODO : make atomic
      for (Schema schema : schemas) {
        StorageMutation mutation = schema.buildDeleteMutation(deletion);
        conn.submit(schema, mutation);
        numDeletes++;
      }
    }
    return numDeletes;
  }

  @Override
  public int update(ValorConnection conn, Collection<Schema> schemas, TupleScanner scanner,
                    Map<String, Object> newVals) throws ValorException, IOException {
    int numUpdates = 0;
    Tuple prev;
    while ((prev = scanner.next()) != null) {
      // TODO : make atomic
      Tuple post = prev.getCopy();
      for (Map.Entry<String, Object> newVal : newVals.entrySet()) {
        post.setAttribute(newVal.getKey(), newVal.getValue());
      }
      for (Schema schema : schemas) {
        StorageMutation mutation = schema.buildUpdateMutation(prev, post);
        conn.submit(schema, mutation);
        numUpdates++;
      }
    }
    return numUpdates;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SchemaHandlerBase that = (SchemaHandlerBase) o;
    return Objects.equals(conf, that.conf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(conf);
  }
}
