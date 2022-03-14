package jp.co.cyberagent.valor.cli;

import jp.co.cyberagent.valor.spi.ValorConnection;
import org.kohsuke.args4j.Argument;

/**
 *
 */
public class DropRelationCommand implements ClientCommand {
  @Argument(index = 0)
  private String relationId;

  @Override
  public int execute(ValorConnection client) throws Exception {
    client.dropRelation(relationId);
    return 0;
  }
}
