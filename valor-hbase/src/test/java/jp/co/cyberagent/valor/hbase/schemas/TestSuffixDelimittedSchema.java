package jp.co.cyberagent.valor.hbase.schemas;

import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.FAMILY;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.QUALIFIER;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.ROWKEY;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.TABLE;
import static jp.co.cyberagent.valor.hbase.storage.HBaseCell.VALUE;
import static jp.co.cyberagent.valor.spi.storage.StorageScan.END_BYTES;
import static jp.co.cyberagent.valor.spi.storage.StorageScan.START_BYTES;
import static jp.co.cyberagent.valor.spi.util.ByteTestUtil.assertBytesEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import jp.co.cyberagent.valor.hbase.storage.HBaseScan;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.hbase.storage.SingleTableHBaseConnection;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.holder.SuffixHolder;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.NotEqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.RegexpOperator;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class TestSuffixDelimittedSchema {

  private final ValorConf conf = new ValorConfImpl();

  private final Relation relation;
  private final Schema schema;

  public TestSuffixDelimittedSchema() throws ValorException {
    relation = ImmutableRelation.builder().relationId("r")
        .addAttribute("k1", true, StringAttributeType.INSTANCE)
        .addAttribute("k2", true, StringAttributeType.INSTANCE)
        .addAttribute("k3", true, IntegerAttributeType.INSTANCE)
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
                SuffixHolder.create("-", AttributeValueFormatter.create("k1")),
                SuffixHolder.create("-", AttributeValueFormatter.create("k2")),
                AttributeValueFormatter.create("k3")))
            .addField(FAMILY, Arrays.asList(ConstantFormatter.create("fam")))
            .addField(QUALIFIER, Arrays.asList(AttributeValueFormatter.create("v1")))
            .addField(VALUE, Arrays.asList(AttributeValueFormatter.create("v2")))
            .build();

    Storage storage = new HBaseStorage(conf);
    schema = storage.buildSchema(relation, descriptor);
  }

  @Test
  public void testBuildSchemaScan() throws Exception {
    PredicativeExpression cond = AndOperator.join(
        new GreaterthanorequalOperator("k1", StringAttributeType.INSTANCE, "begin"),
        new RegexpOperator("k2", StringAttributeType.INSTANCE, "regexp")
    );

    SchemaScan ss = schema.buildScan(Arrays.asList("k1", "k2", "v1", "v2"), cond);
    assertThat(ss.getFragments(), hasSize(1));
    StorageScan fragment = ss.getFragments().get(0);
    assertArrayEquals(ByteUtils.toBytes("begin"), fragment.getStart(ROWKEY));
    assertBytesEquals(ByteUtils.toBytes(".*regexp-.*"), fragment.getFieldComparator(ROWKEY).getRegexp());
  }

  @Test
  public void testRangeAndFilter() throws Exception {
    PredicativeExpression cond = AndOperator.join(
        new GreaterthanorequalOperator("k1", StringAttributeType.INSTANCE, "begin"),
        new EqualOperator("k2", StringAttributeType.INSTANCE, "key2")
    );

    SchemaScan ss = schema.buildScan(Arrays.asList("k1", "k2", "v1", "v2"), cond);
    assertThat(ss.getFragments(), hasSize(1));
    StorageScan fragment = ss.getFragments().get(0);
    assertArrayEquals(ByteUtils.toBytes("begin"), fragment.getStart(ROWKEY));
    assertBytesEquals(ByteUtils.toBytes(".*key2-.*"), fragment.getFieldComparator(ROWKEY).getRegexp());

    HBaseScan hs = SingleTableHBaseConnection.toHBaseScan(TableName.valueOf(TABLE), fragment);
    Scan scan = hs.getScan();
    assertBytesEquals(ByteUtils.toBytes("begin"), scan.getStartRow());
    FilterList filterList = (FilterList) scan.getFilter();
    assertEquals(1, filterList.getFilters().size());
    assertTrue(filterList.getFilters().get(0) instanceof RowFilter);
    RowFilter filter = (RowFilter) filterList.getFilters().get(0);
    assertEquals(CompareFilter.CompareOp.EQUAL, filter.getOperator());
    assertTrue(filter.getComparator() instanceof RegexStringComparator);
    RegexStringComparator comparator = (RegexStringComparator) filter.getComparator();
    assertBytesEquals(ByteUtils.toBytes(".*key2-.*"), comparator.getValue());
  }

  @Test
  public void testRangeWithoutFilter() throws Exception {
    // only string match is pushed down as RegexpStringComparator
    PredicativeExpression cond = AndOperator.join(
        new GreaterthanorequalOperator("k1", StringAttributeType.INSTANCE, "begin"),
        new EqualOperator("k3", IntegerAttributeType.INSTANCE, 100)
    );

    SchemaScan ss = schema.buildScan(Arrays.asList("k1", "k2", "k3", "v1", "v2"), cond);
    assertThat(ss.getFragments(), hasSize(1));
    StorageScan fragment = ss.getFragments().get(0);
    assertArrayEquals(ByteUtils.toBytes("begin"), fragment.getStart(ROWKEY));
    assertNull(fragment.getFieldComparator(ROWKEY).getRegexp());

    HBaseScan hs = SingleTableHBaseConnection.toHBaseScan(TableName.valueOf(TABLE), fragment);
    Scan scan = hs.getScan();
    assertBytesEquals(ByteUtils.toBytes("begin"), scan.getStartRow());
    FilterList filterList = (FilterList) scan.getFilter();
    assertNull(filterList);
  }

  @Test
  public void testScanForNotCondition() throws Exception {
    PredicativeExpression cond = AndOperator.join(
        new NotEqualOperator("k1", StringAttributeType.INSTANCE, "ignore"),
        new NotEqualOperator("v1", StringAttributeType.INSTANCE, "ignore"),
        new NotEqualOperator("v2", StringAttributeType.INSTANCE, "ignore")
    );

    SchemaScan ss = schema.buildScan(Arrays.asList("k1", "v1", "v2"), cond);
    assertThat(ss.getFragments(), hasSize(1));
    StorageScan fragment = ss.getFragments().get(0);

    assertArrayEquals(START_BYTES, fragment.getStart(ROWKEY));
    assertArrayEquals(END_BYTES, fragment.getStop(ROWKEY));
  }

  @Test
  public void testScanForNotConditionForRowSuffix() throws Exception {
    PredicativeExpression cond = AndOperator.join(
        new EqualOperator("k1", StringAttributeType.INSTANCE, "equal"),
        new NotEqualOperator("k2", StringAttributeType.INSTANCE, "ignore")
    );

    SchemaScan ss = schema.buildScan(Arrays.asList("k1", "k2", "v1", "v2"), cond);
    assertThat(ss.getFragments(), hasSize(1));
    StorageScan fragment = ss.getFragments().get(0);
    assertArrayEquals(ByteUtils.toBytes("equal-"), fragment.getStart(ROWKEY));
    assertArrayEquals(ByteUtils.unsignedCopyAndIncrement(ByteUtils.toBytes("equal-")),
        fragment.getStop(ROWKEY));
  }

  @Test
  public void testKeyOnlyFilter() throws Exception {
    PredicativeExpression cond = AndOperator.join(
        new GreaterthanorequalOperator("k1", StringAttributeType.INSTANCE, "begin"),
        new RegexpOperator("k2", StringAttributeType.INSTANCE, "regexp")
    );

    SchemaScan ss = schema.buildScan(Arrays.asList("k1", "k2"), cond);
    assertThat(ss.getFragments(), hasSize(1));
    StorageScan fragment = ss.getFragments().get(0);
    assertArrayEquals(ByteUtils.toBytes("begin"), fragment.getStart(ROWKEY));
    assertBytesEquals(ByteUtils.toBytes(".*regexp-.*"), fragment.getFieldComparator(ROWKEY).getRegexp());
    HBaseScan hs = SingleTableHBaseConnection.toHBaseScan(TableName.valueOf("tbl"), fragment);
    Scan scan = hs.getScan();
    FilterList filter = (FilterList) scan.getFilter();
    assertThat(filter.getFilters(), hasSize(2));
    assertThat(filter.getFilters(), hasItem(instanceOf(KeyOnlyFilter.class)));
    assertThat(filter.getFilters(), hasItem(instanceOf(RowFilter.class)));
  }

}
