package jp.co.cyberagent.valor.hbase;

import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.FAMILY;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.QUALIFIER;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.ROWKEY;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.TABLE;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.RegexpOperator;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class TestKeyMapSchema {

  private final ValorConf conf = new ValorConfImpl();

  private final Relation relation;
  private final Schema schema;

  private final MapAttributeType mapAttributeType;

  public TestKeyMapSchema() throws ValorException {
    mapAttributeType = new MapAttributeType();
    mapAttributeType.addGenericElementType(StringAttributeType.INSTANCE);
    mapAttributeType.addGenericElementType(StringAttributeType.INSTANCE);

    relation = ImmutableRelation.builder().relationId("r")
        .addAttribute("k1", true, StringAttributeType.INSTANCE)
        .addAttribute("k2", true, mapAttributeType)
        .addAttribute("v1", false, StringAttributeType.INSTANCE)
        .build();
    SchemaDescriptor descriptor =
        ImmutableSchemaDescriptor.builder()
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(new ValorConfImpl())
            .isPrimary(false).schemaId("schema")
            .addField(TABLE, Arrays.asList(ConstantFormatter.create("tbl")))
            .addField(ROWKEY, Arrays.asList(AttributeValueFormatter.create("k1")))
            .addField(FAMILY, Arrays.asList(ConstantFormatter.create("fam")))
            .addField(QUALIFIER, Arrays.asList(AttributeValueFormatter.create("k2")))
            .addField(VALUE, Arrays.asList(AttributeValueFormatter.create("v1")))
            .build();

    Storage storage = new HBaseStorage(conf);
    schema = storage.buildSchema(relation, descriptor);
  }

  @Test
  public void testScanForLikeOnMap() throws Exception {
    PredicativeExpression cond = AndOperator.join(
        new EqualOperator("k1", StringAttributeType.INSTANCE, "row"),
        new RegexpOperator("k2", mapAttributeType, new HashMap<String, String>() {
          {
            put("k", "v");
            put("p", "%");
          }
        })
    );

    SchemaScan ss = schema.buildScan(Arrays.asList("k1", "k2", "v1"), cond);
    assertThat(ss.getFragments(), hasSize(1));
    StorageScan fragment = ss.getFragments().get(0);
    assertArrayEquals(ByteUtils.toBytes("row"), fragment.getStart(ROWKEY));
    assertArrayEquals(ByteUtils.unsignedCopyAndIncrement(ByteUtils.toBytes("row")),
        fragment.getStop(ROWKEY));
    assertArrayEquals(toBytes("k", "v", "p", "%"), fragment.getRegexp(QUALIFIER));
  }

  private byte[] toBytes(String... strings) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    for (String s : strings) {
      if (s == null) {
        ByteUtils.writeVInt(dos, -1);
      } else {
        ByteUtils.writeByteArray(dos, ByteUtils.toBytes(s));
      }
    }
    return baos.toByteArray();
  }
}
