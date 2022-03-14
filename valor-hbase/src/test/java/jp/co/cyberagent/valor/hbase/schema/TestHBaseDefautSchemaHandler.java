package jp.co.cyberagent.valor.hbase.schema;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeNamesFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeValuesFormatter;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestHBaseDefautSchemaHandler {

  private static final String TABLE_NAME = "tableName";
  private static final String FAMILY_NAME = "f";

  private static Map<String, Object> attachRelIdConf;

  @BeforeAll
  public static void init() {
    attachRelIdConf = new HashMap<>();
    attachRelIdConf.put(HBaseDefaultSchemaHandler.ATTACH_RELID_AS_PREFIX_KEY, true);
    attachRelIdConf.put(HBaseStorage.TABLE_PARAM_KEY, TABLE_NAME);
    attachRelIdConf.put(HBaseStorage.FAMILY_PARAM_KEY, FAMILY_NAME);
  }

  public static Arguments[] getFixtures() {
    ValorConf c1 = new ValorConfImpl();
    c1.set(HBaseStorage.TABLE_PARAM_KEY, TABLE_NAME);
    Relation r1 = ImmutableRelation.builder().relationId("r1")
        .addAttribute("k", true , StringAttributeType.INSTANCE)
        .addAttribute("v1", false, IntegerAttributeType.INSTANCE)
        .addAttribute("v2", false, LongAttributeType.INSTANCE)
        .build();
    SchemaDescriptor s1 = ImmutableSchemaDescriptor.builder().schemaId("s1")
        .storageClassName(HBaseStorage.class.getCanonicalName())
        .storageConf(c1)
        .conf(new ValorConfImpl())
        .isPrimary(true)
        .addField(HBaseCell.ROWKEY, Arrays.asList(
            VintSizePrefixHolder.create(new ConstantFormatter("r1")),
            VintSizePrefixHolder.create(AttributeValueFormatter.create("k"))))
        .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create(FAMILY_NAME)))
        .addField(HBaseCell.QUALIFIER, Arrays.asList(MultiAttributeNamesFormatter.create("k")))
        .addField(HBaseCell.VALUE, Arrays.asList(MultiAttributeValuesFormatter.create("k")))
        .build();

    return new Arguments[] {
      Arguments.of(r1, s1, attachRelIdConf)
    };

  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void test(Relation relation, SchemaDescriptor expectedOrg, Map<String, Object> conf) throws Exception {
    // set schemaId since default schema build by the handler uses UUID for schemaID
    final String schemaId = "s";
    SchemaDescriptor expected = ImmutableSchemaDescriptor.builder()
        .from(expectedOrg).schemaId(schemaId).build();
    SchemaDescriptor actual = ImmutableSchemaDescriptor.builder()
        .from(HBaseDefaultSchemaHandler.buildSchema(relation, conf)).schemaId(schemaId).build();
    assertEquals(expected, actual);
  }

}
