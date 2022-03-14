package jp.co.cyberagent.valor.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.StringJoiner;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.MapOptionHandler;
import org.kohsuke.args4j.spi.SubCommand;
import org.kohsuke.args4j.spi.SubCommandHandler;
import org.kohsuke.args4j.spi.SubCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValorTool {

  private static final Logger LOG = LoggerFactory.getLogger(ValorTool.class);

  @Option(name = "-c", usage = "config file")
  private File configPath = new File(
      new StringJoiner(System.getProperty("file.separator")).add(System.getProperty("user.dir"))
          .add("conf").add("config.json").toString());

  @Option(name = "-X", handler = MapOptionHandler.class)
  private Map<String, String> conf;

  @Argument(handler = SubCommandHandler.class)
  @SubCommands({
          @SubCommand(name = "show", impl = ShowCommand.class),
          @SubCommand(name = "createRelation", impl = RegisterRelationCommand.class),
          @SubCommand(name = "dropRelation", impl = DropRelationCommand.class),
          @SubCommand(name = "createSchema", impl = RegisterSchemaCommand.class),
          @SubCommand(name = "insert", impl = InsertCommand.class),
          @SubCommand(name = "search", impl = SearchCommand.class),
          @SubCommand(name = "count", impl = CountCommand.class),
          @SubCommand(name = "describe", impl = DescribeCommand.class),
          @SubCommand(name = "optimize", impl = OptimizeCommand.class),
          @SubCommand(name = "ql", impl = QueryCommand.class),
          @SubCommand(name = "webapi", impl = WebapiCommand.class)
  })
  private Command command;

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws IOException, ValorException {
    ValorTool tool = new ValorTool();
    CmdLineParser parser = new CmdLineParser(tool);
    try {
      parser.parseArgument(args);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      System.exit(-1);
    }
    Map<String, String> conf;
    if (tool.conf != null) {
      conf = tool.conf;
    } else {
      conf = new ObjectMapper().readValue(tool.configPath, Map.class);
    }
    LOG.info("config:");
    conf.entrySet().forEach(e -> {
      LOG.info("  {}={}", e.getKey(), e.getValue());
    });
    int exitCode = tool.command.execute(conf);
    System.exit(exitCode);
  }
}
