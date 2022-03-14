package jp.co.cyberagent.valor.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.Test;
import org.kohsuke.args4j.CmdLineParser;

public class TestDescribeCommand {
  private final String RELATION_ID = "test";
  private final String SCHEMA_ID = "s";
  private final String FIELD_NAME = "field_name";
  private final String OUTPUT_FILE = System.getProperty("java.io.tmpdir") + "testSchema.json";

  @Test
  public void testExecute() throws Exception {
    ValorConf conf = new ValorConfImpl();
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, InMemorySchemaRepository.NAME);
    ValorContext context = StandardContextFactory.create(conf);
    ValorConnection client = ValorConnectionFactory.create(context);

    Relation relation = ImmutableRelation.builder().relationId(RELATION_ID)
        .addAttribute("k1", true, StringAttributeType.INSTANCE)
        .addAttribute("a1", false, StringAttributeType.INSTANCE)
        .build();
    client.createRelation(relation, true);

    SchemaDescriptor descriptor = ImmutableSchemaDescriptor.builder().isPrimary(true)
        .schemaId(SCHEMA_ID)
        .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
        .storageConf(conf)
        .conf(conf)
        .addField(FIELD_NAME, Arrays.asList(ConstantFormatter.create("t")))
        .build();
    client.createSchema(RELATION_ID, descriptor, true);
    Schema schema = client.findSchema(RELATION_ID, SCHEMA_ID);

    DescribeCommand describeCommand = new DescribeCommand();
    CmdLineParser parser = new CmdLineParser(describeCommand);
    String[] args = {"-r", "test"};
    parser.parseArgument(args);
    String actural;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      System.setOut(new PrintStream(baos));
      describeCommand.execute(client);
      actural = new String(baos.toByteArray());
    }
    Object expected;
    MetadataJsonSerde serde = new MetadataJsonSerde(context);
    byte[] buf = serde.serialize(descriptor);
    ObjectMapper om = new ObjectMapper();
    Object out = om.readValue(buf, Object.class);
    expected = om.writerWithDefaultPrettyPrinter().writeValueAsString(out);
    assertEquals(expected + "\n", actural);
  }
}
