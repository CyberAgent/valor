package jp.co.cyberagent.valor.cli;

import java.io.FileInputStream;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.metadata.MetadataSerde;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.kohsuke.args4j.Option;

/**
 *
 */
public class RegisterSchemaCommand implements ClientCommand {

  @Option(name = "-r", usage = "relationId", required = true)
  private String relationId;

  @Option(name = "-s", usage = "schemaId", required = true)
  private String schemaId;

  @Option(name = "-f", usage = "schema definition file (json)", required = true)
  private String pathToSchemaFile;

  @Override
  public int execute(ValorConnection client) throws Exception {
    ValorContext context = client.getContext();
    MetadataSerde serde = new MetadataJsonSerde(context);
    SchemaDescriptor schema;
    try (FileInputStream fis = new FileInputStream(pathToSchemaFile)) {
      schema = serde.readSchema(schemaId, fis);
      client.createSchema(relationId, schema, true);
    }
    return 0;
  }
}
