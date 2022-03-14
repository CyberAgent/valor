package jp.co.cyberagent.valor.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Map;
import jp.co.cyberagent.valor.cli.optimizer.OptimizationContext;
import jp.co.cyberagent.valor.hbase.HBasePlugin;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.kohsuke.args4j.CmdLineParser;

public class TestOptimizeCommand {

  private static Relation relation = ImmutableRelation.builder().relationId("rel")
      .addAttribute("k1", true, StringAttributeType.INSTANCE)
      .addAttribute("k2", true, StringAttributeType.INSTANCE)
      .addAttribute("v1", false, IntegerAttributeType.INSTANCE)
      .addAttribute("v2", false, IntegerAttributeType.INSTANCE)
      .build();

  private static ValorConnection client;

  @BeforeAll
  public static void setup() throws ValorException {
    ValorContext context = StandardContextFactory.create();
    context.installPlugin(new HBasePlugin());
    client = ValorConnectionFactory.create(context);
    client.createRelation(relation, true);
  }

  @AfterAll
  public static void tearDown() throws IOException {
    client.close();
  }

  @Test
  public void testExecute() throws Exception {
    OptimizeCommand testCommand = new OptimizeCommand();
    CmdLineParser parser = new CmdLineParser(testCommand);
    String[] args = {
        "-f", "src/test/resources/testOptimize.json",
        "-s", "jp.co.cyberagent.valor.sdk.optimize.HeuristicSpaceSolver"
    };
    parser.parseArgument(args);
    assertEquals("src/test/resources/testOptimize.json", testCommand.getPathToQueryFile());
    assertEquals("jp.co.cyberagent.valor.sdk.optimize.HeuristicSpaceSolver", testCommand.getSolverName());
    testCommand.execute(client);
  }

  @Test
  public void testOptimize() throws Exception {
    OptimizeCommand testCommand = new OptimizeCommand();
    OptimizationContext optContext
        = testCommand.readOptimizationContext("src/test/resources/testOptimize.json", client);
    Map<String, ScanPlan> plans =
        testCommand.optimize(client.getContext(), optContext);
    assertThat(plans.keySet(), hasSize(2));
  }
}
