package jp.co.cyberagent.valor.cli;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collection;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.kohsuke.args4j.Option;

/**
 * This command describes the schema definitions for given relationId.
 */
public class DescribeCommand implements ClientCommand {

  @Option(name = "-r", usage = "relationId", required = true)
  private String relationId;

  @Override
  public int execute(ValorConnection client) throws Exception {
    ValorContext context = client.getContext();
    MetadataJsonSerde serde = new MetadataJsonSerde(context);
    Relation relation = client.findRelation(relationId);
    if (relation == null) {
      throw new ValorException("relation " + relationId + " not found");
    }
    Collection<Schema> schemas = client.listSchemas(relationId);
    ObjectMapper om = new ObjectMapper();
    Collection<SchemaDescriptor> sds = schemas.stream()
        .map(SchemaDescriptor::from).collect(Collectors.toList());
    for (SchemaDescriptor sd : sds) {
      byte[] buf = serde.serialize(sd);
      Object out = om.readValue(buf, Object.class);
      System.out.println(om.writerWithDefaultPrettyPrinter().writeValueAsString(out));
    }
    return 0;
  }
}
