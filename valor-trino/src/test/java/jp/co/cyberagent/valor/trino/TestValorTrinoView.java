package jp.co.cyberagent.valor.trino;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import io.trino.Session;
import io.trino.testing.MaterializedResult;
import io.trino.testing.StandaloneQueryRunner;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.Murmur3MapKeyFormatter;
import jp.co.cyberagent.valor.sdk.holder.FixedLengthHolder;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.metadata.FileSchemaRepository;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.ByteArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.DoubleAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.FloatAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Disabled
public class TestValorTrinoView {

  private static final Session SESSION = testSessionBuilder()
          .setCatalog("valor")
          .setSchema("default").build();

  private static StandaloneQueryRunner queryRunner;

  private static SchemaRepository schemaRepository;

  private static File baseDir;


  @SuppressWarnings("unchecked")
  @BeforeAll
  public static void setup() throws Exception {
    String tmpDir = System.getProperty("java.io.tmpdir");
    baseDir = new File(tmpDir, TestValorTrinoView.class.getName());
    baseDir.mkdirs();
    Map<String,String> conf = new HashMap() {
      {
        put(StandardContextFactory.SCHEMA_REPOSITORY_CLASS.name, FileSchemaRepository.NAME);
        put(FileSchemaRepository.SCHEMA_REPOS_BASEDIR.name, baseDir.getAbsolutePath());
      }
    };

    queryRunner = new StandaloneQueryRunner(SESSION);
    queryRunner.installPlugin(new ValorPlugin());
    // workaround of service loader is not working in unit test
    queryRunner.createCatalog("valor", "valor", conf);

    ValorContext context = StandardContextFactory.create(conf);
    schemaRepository = context.createRepository(context.getConf());
  }

  @AfterAll
  public static void tearDown() throws Exception {
    queryRunner.close();
    schemaRepository.close();
    for (File f : baseDir.listFiles()) {
      f.delete();
    }
    baseDir.delete();
  }

  static SchemaDescriptor createDefaultSchema(Relation relation) {
    ImmutableSchemaDescriptor.Builder builder = ImmutableSchemaDescriptor.builder()
        .schemaId("v1")
        .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
        .storageConf(new ValorConfImpl());

    List<Segment> keySegments = new ArrayList<>();
    List<Segment> valueSegments = new ArrayList<>();
    for (String attr : relation.getAttributeNames()) {
      if (relation.isKey(attr)) {
        keySegments.add(VintSizePrefixHolder.create(AttributeValueFormatter.create(attr)));
      } else {
        valueSegments.add(VintSizePrefixHolder.create(AttributeValueFormatter.create(attr)));
      }
    }
    builder.addField(EmbeddedEKVStorage.KEY,
        Arrays.asList(ConstantFormatter.create(relation.getRelationId())));
    builder.addField(EmbeddedEKVStorage.COL, keySegments);
    builder.addField(EmbeddedEKVStorage.VAL, valueSegments);
    return builder.build();
  }

  public static Arguments[] getFixtures() {
    Relation numericRel = ImmutableRelation.builder().relationId("longAttrRel")
        .addAttribute("l", true, LongAttributeType.INSTANCE)
        .addAttribute("i", false, IntegerAttributeType.INSTANCE)
        .addAttribute("d", false, DoubleAttributeType.INSTANCE)
        .addAttribute("f", false, FloatAttributeType.INSTANCE)
        .build();
    Map<String, Number> numVal = new HashMap<>();
    numVal.put("l", 1l);
    numVal.put("i", 2);
    numVal.put("d", 3.0);
    numVal.put("f", 4.0);

    Relation binaryRel = ImmutableRelation.builder().relationId("binaryAttrRel")
        .addAttribute("k", true, ByteArrayAttributeType.INSTANCE)
        .addAttribute("v", false, ByteArrayAttributeType.INSTANCE)
        .build();
    Map<String, byte[]> binaryVal = new HashMap<>();
    binaryVal.put("k", "abc".getBytes(StandardCharsets.UTF_8));
    binaryVal.put("v", "xyz".getBytes(StandardCharsets.UTF_8));


    return new Arguments[] {
        Arguments.of(numericRel, createDefaultSchema(numericRel), numVal),
        Arguments.of(binaryRel, createDefaultSchema(binaryRel), binaryVal)
    };
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void test(Relation relation, SchemaDescriptor schema, Map val) throws Exception {
    schemaRepository.createRelation(relation, true);
    schemaRepository.createSchema(relation.getRelationId(), schema, true);

    final String relId = relation.getRelationId();
    final List<String> attrs = relation.getAttributeNames();
    queryRunner.execute(String.format("describe %s", relId));
    queryRunner.execute(String.format("insert into %s (%s) values (%s)",
        relId,
        attrs.stream().collect(Collectors.joining(", ")),
        attrs.stream().map(val::get).map(v -> toExpString(v)).collect(Collectors.joining(", "))));
    MaterializedResult result = queryRunner.execute(String.format("SELECT * from %s", relId));
    assertThat(result.getRowCount(), equalTo(1));
    for (int i = 0; i < attrs.size(); i++) {
      assertThat(result.getMaterializedRows().get(0).getField(i), equalTo(val.get(attrs.get(i))));
    }

    String keyAttr = relation.getKeyAttributeNames().get(0);
    queryRunner.execute(String.format("DELETE from %s WHERE %s != %s",
        relId, keyAttr, toExpString(val.get(keyAttr))));
    result = queryRunner.execute(String.format("SELECT * from %s", relId));
    assertThat(result.getRowCount(), equalTo(1));
    for (int i = 0; i < attrs.size(); i++) {
      assertThat(result.getMaterializedRows().get(0).getField(i), equalTo(val.get(attrs.get(i))));
    }
    queryRunner.execute(String.format("DELETE from %s WHERE %s = %s",
        relId, keyAttr, toExpString(val.get(keyAttr))));
    result = queryRunner.execute(String.format("SELECT * from %s", relId));
    assertThat(result.getRowCount(), equalTo(0));
  }

  private String toExpString(Object v) {
    if (v instanceof byte[]) {
      String hex = toHexString((byte[]) v);
      return "X'" + hex + "'";
    } else if (v instanceof String) {
      return "'" + v + "'";
    } else {
      return v.toString();
    }
  }

  private String toHexString(byte[] value) {
    StringBuilder buf = new StringBuilder();
    for (byte v : value) {
      buf.append(String.format("%02X", v));
    }
    return buf.toString();
  }

  @Test
  public void testMap() throws Exception {
    MapAttributeType mapType
        = MapAttributeType.create(StringAttributeType.INSTANCE, StringAttributeType.INSTANCE);
    String readRelId = "testMapRead";
    Relation readRelation = ImmutableRelation.builder().relationId(readRelId)
        .addAttribute("h", true, ByteArrayAttributeType.INSTANCE)
        .addAttribute("k", true, mapType)
        .addAttribute("v", false, LongAttributeType.INSTANCE).build();
    SchemaDescriptor readSchema = ImmutableSchemaDescriptor.builder()
        .schemaId("v1")
        .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
        .storageConf(new ValorConfImpl())
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(
            FixedLengthHolder.create(ByteUtils.SIZEOF_INT, AttributeValueFormatter.create("h")),
            AttributeValueFormatter.create("k")
        ))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(ConstantFormatter.create("")))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(AttributeValueFormatter.create("v")))
        .build();

    String writeRelId = "testMapWrite";
    Relation writeRelation = ImmutableRelation.builder().relationId(writeRelId)
        .addAttribute("k", true, mapType)
        .addAttribute("v", false, LongAttributeType.INSTANCE).build();
    SchemaDescriptor writeSchema = ImmutableSchemaDescriptor.builder()
        .schemaId("v1")
        .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
        .storageConf(new ValorConfImpl())
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(
            FixedLengthHolder.create(ByteUtils.SIZEOF_INT, Murmur3MapKeyFormatter.create("k")),
            AttributeValueFormatter.create("k")
        ))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(ConstantFormatter.create("")))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(AttributeValueFormatter.create("v")))
        .build();



    schemaRepository.createRelation(readRelation, true);
    schemaRepository.createSchema(readRelId, readSchema, true);
    schemaRepository.createRelation(writeRelation, true);
    schemaRepository.createSchema(writeRelId, writeSchema, true);

    queryRunner.execute(String.format(
        "insert into %s select MAP(ARRAY['a','b'], ARRAY['x','y']), 100", writeRelId));
    MaterializedResult result = queryRunner.execute(String.format(
        "SELECT * FROM %s WHERE h = murmur3_binary(ARRAY['a', 'b'], 4)", readRelId));
    assertThat(result.getRowCount(), equalTo(1));
  }

}
