package jp.co.cyberagent.valor.cli;

import java.util.Collection;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.schema.Schema;

/**
 *
 */
public class ShowCommand implements ClientCommand {

  @Override
  public int execute(ValorConnection client) throws ValorException {
    Collection<String> relationIds = client.listRelationsIds();
    for (String relationId : relationIds) {
      System.out.println(relationId);
      Collection<Schema> schemas = client.listSchemas(relationId);
      for (Schema schema : schemas) {
        System.out.println("  " + schema.getSchemaId());
      }
    }
    return 0;
  }
}
