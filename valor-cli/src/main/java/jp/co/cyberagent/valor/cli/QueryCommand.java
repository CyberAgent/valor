package jp.co.cyberagent.valor.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import jp.co.cyberagent.valor.ql.parse.Parser;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.DeleteStatement;
import jp.co.cyberagent.valor.spi.plan.model.InsertStatement;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.plan.model.ValueItem;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import org.kohsuke.args4j.Option;

/**
 *
 */
public class QueryCommand implements ClientCommand {

  @Option(name = "-e", usage = "query", required = true)
  private String query;

  @Override
  public int execute(ValorConnection connection) throws Exception {
    Parser parser = new Parser(connection);
    LogicalPlanNode stmt = parser.parseStatement(query);
    if (stmt instanceof RelationScan) {
      executeQuery(connection, (RelationScan) stmt);
    } else if (stmt instanceof InsertStatement) {
      executeInsert(connection, (InsertStatement) stmt);
    } else if (stmt instanceof DeleteStatement) {
      executeDelete(connection, (DeleteStatement) stmt);
    }

    return 0;
  }

  private void executeQuery(ValorConnection connection, RelationScan query)
      throws ValorException, IOException {
    List<ProjectionItem> items = query.getItems();
    int cnt = query.getLimit();
    try (TupleScanner scanner = connection.compile(query)) {
      Tuple tuple;
      while ((tuple = scanner.next()) != null && cnt != 0) {
        for (ProjectionItem i : items) {
          System.out.print(i.apply(tuple));
          System.out.print("\t");
        }
        System.out.println();
        cnt--;
      }
    }
  }

  private void executeInsert(ValorConnection connection, InsertStatement stmt)
      throws ValorException, IOException {
    // TODO support namespace and schema
    Relation relation = stmt.getRelation().getRelation();
    List<String> attrs = stmt.getAttributes();
    if (attrs == null) {
      attrs = relation.getAttributeNames();
    }
    List<ValueItem> values = stmt.getValues();
    List<Tuple> tuples = new ArrayList<>(values.size());
    for (ValueItem value : values) {
      Tuple t = new TupleImpl(relation);
      List<ConstantExpression> vs = value.getValues();
      for (int i = 0; i < vs.size(); i++) {
        String attr = attrs.get(i);
        Object v = vs.get(i).getValue();
        t.setAttribute(attr, v);
      }
      tuples.add(t);
    }
    connection.insert(relation.getRelationId(), tuples);
  }

  private void executeDelete(ValorConnection connection, DeleteStatement stmt)
      throws ValorException, IOException {
    // TODO support namespace
    Relation relation = stmt.getRelation().getRelation();
    connection.delete(null, relation.getRelationId(), stmt.getCondition(), stmt.getLimit());
  }
}
