package jp.co.cyberagent.valor.hbase.schemas;

import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.FAMILY;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.QUALIFIER;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.ROWKEY;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.TABLE;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.IsNullOperator;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
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
public class TestSizePrefixedSchema {

  private final ValorConf conf = new ValorConfImpl();

  private final Relation relation;
  private final Schema schema;

  public TestSizePrefixedSchema() throws ValorException {
    relation = ImmutableRelation.builder().relationId("r")
        .addAttribute("k1", true, StringAttributeType.INSTANCE)
        .addAttribute("k2", true, StringAttributeType.INSTANCE)
        .addAttribute("v1", false, StringAttributeType.INSTANCE)
        .addAttribute("v2", false, StringAttributeType.INSTANCE)
        .build();
    SchemaDescriptor descriptor =
        ImmutableSchemaDescriptor.builder()
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(new ValorConfImpl())
            .isPrimary(false).schemaId("schema")
            .addField(TABLE, Arrays.asList(ConstantFormatter.create("tbl")))
            .addField(ROWKEY, Arrays.asList(
                VintSizePrefixHolder.create(AttributeValueFormatter.create("k1")),
                VintSizePrefixHolder.create(AttributeValueFormatter.create("v2")),
                AttributeValueFormatter.create("k2")))
            .addField(FAMILY, Arrays.asList(ConstantFormatter.create("fam")))
            .addField(QUALIFIER, Arrays.asList(AttributeValueFormatter.create("v1")))
            .addField(VALUE, Arrays.asList(AttributeValueFormatter.create("v2")))
            .build();

    Storage storage = new HBaseStorage(conf);
    schema = storage.buildSchema(relation, descriptor);

  }

  @Test
  public void testSizePrefixedSchemaScan() throws Exception {
    byte[] valOfKey1 = ByteUtils.toBytes("key1");
    byte[] valOfKey2 = ByteUtils.toBytes("key2");
    byte[] valOfAnony1 = ByteUtils.toBytes("val2");
    PredicativeExpression cond = AndOperator.join(
        new EqualOperator("k1", StringAttributeType.INSTANCE, ByteUtils.toString(valOfKey1)),
        new EqualOperator("k2", StringAttributeType.INSTANCE, ByteUtils.toString(valOfKey2)),
        new EqualOperator("v2", StringAttributeType.INSTANCE, ByteUtils.toString(valOfAnony1))
    );

    SchemaScan ss = schema.buildScan(Arrays.asList("k1", "k2", "v1", "v2"), cond);
    assertThat(ss.getFragments(), hasSize(1));
    StorageScan fragment = ss.getFragments().get(0);

    byte[] expectedStartRow = ByteUtils.add(new byte[] {(byte) valOfKey1.length}, valOfKey1);
    expectedStartRow = ByteUtils.add(expectedStartRow, new byte[] {(byte) valOfAnony1.length},
        valOfAnony1);
    expectedStartRow = ByteUtils.add(expectedStartRow, valOfKey2);
    byte[] expectedStopRow = ByteUtils.unsignedCopyAndIncrement(expectedStartRow);
    assertArrayEquals(expectedStartRow, fragment.getStart(ROWKEY));
    assertArrayEquals(expectedStopRow, fragment.getStop(ROWKEY));

    // test null on head
    cond = new IsNullOperator(new AttributeNameExpression("k1", StringAttributeType.INSTANCE));

    ss = schema.buildScan(Arrays.asList("k1", "k2", "v1", "v2"), cond);
    assertThat(ss.getFragments(), hasSize(1));
    fragment = ss.getFragments().get(0);

    expectedStartRow = new byte[] {(byte) 0xff};
    assertArrayEquals(expectedStartRow, fragment.getStart(ROWKEY));
    assertThat(fragment.getStop(ROWKEY), is(nullValue()));
  }
}
