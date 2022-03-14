package jp.co.cyberagent.valor.hbase.schemas;

import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.FAMILY;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.QUALIFIER;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.ROWKEY;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.TABLE;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.VALUE;
import static jp.co.cyberagent.valor.spi.util.ByteTestUtil.assertBytesEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.String2DateTimeFrameFormatter;
import jp.co.cyberagent.valor.sdk.holder.FixedLengthHolder;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class TestDateTimeRangeSchema {

  private final ValorConf conf = new ValorConfImpl();

  private final Relation relation;
  private final Schema schema;

  public TestDateTimeRangeSchema() throws ValorException {
    relation = ImmutableRelation.builder().relationId("r")
        .addAttribute("k1", true, StringAttributeType.INSTANCE)
        .addAttribute("k2", true, StringAttributeType.INSTANCE)
        .addAttribute("date", true, StringAttributeType.INSTANCE)
        .addAttribute("val", false, StringAttributeType.INSTANCE)
        .build();
    Segment dateSchemaElement =
        FixedLengthHolder.create(String2DateTimeFrameFormatter.BYTE_LENGTH,
            new String2DateTimeFrameFormatter("date"));
    SchemaDescriptor descriptor =
        ImmutableSchemaDescriptor.builder()
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(new ValorConfImpl())
            .isPrimary(false).schemaId("schema")
            .addField(TABLE, Arrays.asList(ConstantFormatter.create("tbl")))
            .addField(ROWKEY, Arrays.asList(
                VintSizePrefixHolder.create(AttributeValueFormatter.create("k1")),
                VintSizePrefixHolder.create(AttributeValueFormatter.create("k2")),
                dateSchemaElement))
            .addField(FAMILY, Arrays.asList(ConstantFormatter.create("fam")))
            .addField(QUALIFIER, Arrays.asList(ConstantFormatter.create("qual")))
            .addField(VALUE, Arrays.asList(AttributeValueFormatter.create("val")))
            .build();

    Storage storage = new HBaseStorage(conf);
    schema = storage.buildSchema(relation, descriptor);
  }

  @Test
  public void testDateTimeRangeSchemaScan() throws Exception {
    byte[] valOfKey1 = ByteUtils.toBytes("key1");
    byte[] valOfKey2 = ByteUtils.toBytes("key2");
    PredicativeExpression cond = AndOperator.join(
        new EqualOperator("k1", StringAttributeType.INSTANCE, ByteUtils.toString(valOfKey1)),
        new EqualOperator("k2", StringAttributeType.INSTANCE, ByteUtils.toString(valOfKey2)),
        new EqualOperator("date", StringAttributeType.INSTANCE, "2013-12-09")
    );

    SchemaScan ss = schema.buildScan(Arrays.asList("k1", "k2", "date", "val"), cond);
    assertThat(ss.getFragments(), hasSize(1));
    StorageScan fragment = ss.getFragments().get(0);
    byte[] expectedStartRow = ByteUtils.add(new byte[] {(byte) valOfKey1.length}, valOfKey1);
    expectedStartRow = ByteUtils.add(expectedStartRow, new byte[] {(byte) valOfKey2.length}, valOfKey2);

    byte[] dtf = new byte[] {String2DateTimeFrameFormatter.FrameType.DAILY.value()};
    dtf = ByteUtils.add(dtf, ByteUtils.toBytes((short) 2013), new byte[] {(byte) 12, (byte) 9, (byte) -1,
        (byte) -1, (byte) -1});

    expectedStartRow = ByteUtils.add(expectedStartRow, dtf);
    byte[] expectedStopRow = ByteUtils.unsignedCopyAndIncrement(expectedStartRow);
    assertBytesEquals(expectedStartRow, fragment.getStart(ROWKEY));
    assertBytesEquals(expectedStopRow, fragment.getStop(ROWKEY));

    byte[] dtf1 = new byte[] {String2DateTimeFrameFormatter.FrameType.DAILY.value()};
    dtf1 = ByteUtils.add(dtf1, ByteUtils.toBytes((short) 2013), new byte[] {(byte) 12, (byte) 10,
        (byte) -1, (byte) -1, (byte) -1});
    byte[] dtf2 = new byte[] {String2DateTimeFrameFormatter.FrameType.DAILY.value()};
    dtf2 = ByteUtils.add(dtf2, ByteUtils.toBytes((short) 2013), new byte[] {(byte) 12, (byte) 20,
        (byte) -1, (byte) -1, (byte) -1});
    testDateFrame(schema, "2013-12-10", dtf1, "2013-12-20", dtf2);

    dtf1 = new byte[] {String2DateTimeFrameFormatter.FrameType.MONTHLY.value()};
    dtf1 = ByteUtils.add(dtf1, ByteUtils.toBytes((short) 2013), new byte[] {(byte) 12, (byte) -1,
        (byte) -1, (byte) -1, (byte) -1});
    dtf2 = new byte[] {String2DateTimeFrameFormatter.FrameType.MONTHLY.value()};
    dtf2 = ByteUtils.add(dtf2, ByteUtils.toBytes((short) 2014), new byte[] {(byte) 01, (byte) -1,
        (byte) -1, (byte) -1, (byte) -1});
    testDateFrame(schema, "2013-12", dtf1, "2014-01", dtf2);
  }

  protected void testDateFrame(Schema def, String beginStr, byte[] beginBytes, String endStr,
                               byte[] endBytes) throws Exception {
    byte[] valOfKey1 = ByteUtils.toBytes("key1");
    byte[] valOfKey2 = ByteUtils.toBytes("key2");
    byte[] expectedStartRow;
    byte[] expectedStopRow;

    PredicativeExpression condRange = AndOperator.join(
        new EqualOperator("k1", StringAttributeType.INSTANCE, ByteUtils.toString(valOfKey1)),
        new EqualOperator("k2", StringAttributeType.INSTANCE, ByteUtils.toString(valOfKey2)),
        new GreaterthanorequalOperator("date", StringAttributeType.INSTANCE, beginStr),
        new LessthanorequalOperator("date", StringAttributeType.INSTANCE, endStr)
    );
    SchemaScan ss = def.buildScan(Arrays.asList("k1", "k2", "date"), condRange);
    assertThat(ss.getFragments(), hasSize(1));
    StorageScan fragment = ss.getFragments().get(0);
    expectedStartRow = ByteUtils.add(new byte[] {(byte) valOfKey1.length}, valOfKey1);
    expectedStartRow = ByteUtils.add(expectedStartRow, new byte[] {(byte) valOfKey2.length}, valOfKey2);
    expectedStartRow = ByteUtils.add(expectedStartRow, beginBytes);
    expectedStopRow = ByteUtils.add(new byte[] {(byte) valOfKey1.length}, valOfKey1);
    expectedStopRow = ByteUtils.add(expectedStopRow, new byte[] {(byte) valOfKey2.length}, valOfKey2);
    expectedStopRow = ByteUtils.add(expectedStopRow, endBytes);
    expectedStopRow = ByteUtils.unsignedCopyAndIncrement(expectedStopRow);

    assertArrayEquals(expectedStartRow, fragment.getStart(ROWKEY));
    assertArrayEquals(expectedStopRow, fragment.getStop(ROWKEY));
  }

}
