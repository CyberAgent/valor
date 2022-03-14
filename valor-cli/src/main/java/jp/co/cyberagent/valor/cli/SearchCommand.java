package jp.co.cyberagent.valor.cli;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.File;
import java.util.List;
import java.util.Scanner;
import jp.co.cyberagent.valor.cli.ast.AllItemNode;
import jp.co.cyberagent.valor.cli.ast.AstNodeDeserializer;
import jp.co.cyberagent.valor.cli.ast.ExpressionNode;
import jp.co.cyberagent.valor.cli.ast.ProjectionClauseNode;
import jp.co.cyberagent.valor.cli.ast.RelationSourceNode;
import jp.co.cyberagent.valor.cli.ast.ScanNode;
import jp.co.cyberagent.valor.cli.ast.SemanticAnalyzer;
import jp.co.cyberagent.valor.cli.ast.WhereClauseNode;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import org.kohsuke.args4j.Option;

/***
 * @deprecated use {@link QueryCommand}
 */
@Deprecated
public class SearchCommand implements ClientCommand {

  @Option(name = "-f", usage = "query file (json)", required = true)
  private String pathToQueryFile;

  @Option(name = "-r", usage = "relation id", required = true)
  private String relationId;

  @Override
  public int execute(ValorConnection client) throws Exception {
    Module module = new SimpleModule()
        .addDeserializer(ExpressionNode.class, new AstNodeDeserializer.ExpressionDeserializer());
    ObjectMapper mapper = new ObjectMapper().registerModule(module);
    try (Scanner scanner = new Scanner(new File(pathToQueryFile))) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine();
        ExpressionNode ast = mapper.readValue(line, ExpressionNode.class);
        ScanNode queryNode = new ScanNode(
            new ProjectionClauseNode(new AllItemNode()),
            new RelationSourceNode(relationId),
            new WhereClauseNode(ast));
        SchemaRepository repository = client.getSchemaRepository();
        RelationScan query = (RelationScan) new SemanticAnalyzer(repository).walk(queryNode);
        PredicativeExpression conditions = query.getCondition();
        System.out.println(conditions.toString());

        List<Tuple> selectResults = client.scan(query);

        System.out.println("results: ");
        for (Tuple result : selectResults) {
          System.out.println(result.toString());
        }
      }
    }
    return 0;
  }

}
