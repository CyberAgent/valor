package jp.co.cyberagent.valor.spi.schema;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;

public interface SchemaHandler {

  String getName();

  Map<String, Object> getConf();

  SchemaHandler init(Relation relation);

  SchemaHandler configure(Map<String, Object> conf);

  ScanPlan plan(ValorConnection conn,
                Collection<Schema> schemas,
                List<ProjectionItem> items,
                PredicativeExpression condition,
                Planner planner) throws ValorException;

  TupleScanner scanner(
      ValorConnection conn,
      Collection<Schema> schemas,
      List<ProjectionItem> items,
      PredicativeExpression condition,
      Planner planner) throws ValorException;

  void insert(ValorConnection conn, Collection<Schema> schemas, Collection<Tuple> tuples)
      throws ValorException, IOException;

  int delete(ValorConnection conn, Collection<Schema> schemas, TupleScanner scanner, Integer limit)
      throws ValorException, IOException;

  int update(ValorConnection conn,
             Collection<Schema> schemas, TupleScanner scanner, Map<String, Object> newVals)
      throws ValorException, IOException;
}
