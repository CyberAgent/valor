package jp.co.cyberagent.valor.cli;

import java.io.FileInputStream;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.metadata.MetadataSerde;
import jp.co.cyberagent.valor.spi.relation.Relation;
import org.kohsuke.args4j.Option;

/**
 *
 */
public class RegisterRelationCommand implements ClientCommand {

  @Option(name = "-r", usage = "relationId", required = true)
  private String relationId;

  @Option(name = "-f", usage = "relation definition file (json)", required = true)
  private String pathToRelationFile;

  @Override
  public int execute(ValorConnection client) throws Exception {
    ValorContext context = client.getContext();
    MetadataSerde serde = new MetadataJsonSerde(context);
    Relation relation;
    try (FileInputStream fis = new FileInputStream(pathToRelationFile)) {
      relation = serde.readRelation(relationId, fis);
      client.createRelation(relation, true);
    }
    return 0;
  }
}
