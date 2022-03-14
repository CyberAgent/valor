package jp.co.cyberagent.valor.hbase.schemas;

import static jp.co.cyberagent.valor.sdk.formatter.MultiAttributeNamesFormatter.EMPTY_MARKER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeNamesFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeValuesFormatter;
import jp.co.cyberagent.valor.sdk.serde.ContinuousRecordsDeserializer;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class TestMultiKVValueDefinition {
  public static final String ROW = "row";
  private static final String FAMILY = "data";
  private final Relation relation;
  private Schema schema;
  private String tableName;

  public TestMultiKVValueDefinition() throws Exception {
    ValorConf conf = new ValorConfImpl();
    relation = ImmutableRelation.builder().relationId("mkb")
        .addAttribute(ROW, true, StringAttributeType.INSTANCE)
        .addAttribute("p1", false, StringAttributeType.INSTANCE)
        .addAttribute("p2", false, StringAttributeType.INSTANCE)
        .addAttribute("p3", false, StringAttributeType.INSTANCE).build();

    tableName = "table";
    SchemaDescriptor descriptor = ImmutableSchemaDescriptor.builder()
        .schemaId("test")
        .storageClassName(HBaseStorage.class.getCanonicalName())
        .storageConf(conf)
        .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create(tableName)))
        .addField(HBaseCell.ROWKEY, Arrays.asList(AttributeValueFormatter.create(ROW)))
        .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create(FAMILY)))
        .addField(HBaseCell.QUALIFIER, Arrays.asList(MultiAttributeNamesFormatter.create(ROW)))
        .addField(HBaseCell.VALUE, Arrays.asList(MultiAttributeValuesFormatter.create(ROW)))
        .build();
    Storage storage = new HBaseStorage(conf);
    schema = storage.buildSchema(relation, descriptor);
  }

  @Test
  public void testMultiProps() throws Exception {

    Tuple val = new TupleImpl(relation);
    val.setAttribute(ROW, "r1");
    val.setAttribute("p1", "v1");
    val.setAttribute("p2", "v2");
    val.setAttribute("p3", "");

    List<Record> records = schema.serialize(val);
    assertEquals(3, records.size());

    for (Record record : records) {
      assertEquals("r1", ByteUtils.toString(record.getBytes(HBaseCell.ROWKEY)));
      if (ByteUtils.equals(ByteUtils.toBytes("p1"), record.getBytes(HBaseCell.QUALIFIER))) {
        assertEquals("v1", ByteUtils.toString(record.getBytes(HBaseCell.VALUE)));
      } else if (ByteUtils.equals(ByteUtils.toBytes("p2"), record.getBytes(HBaseCell.QUALIFIER))) {
        assertEquals("v2", ByteUtils.toString(record.getBytes(HBaseCell.VALUE)));
      } else if (ByteUtils.equals(ByteUtils.toBytes("p3"), record.getBytes(HBaseCell.QUALIFIER))) {
        assertEquals("", ByteUtils.toString(record.getBytes(HBaseCell.VALUE)));
      } else {
        fail();
      }
    }
    ContinuousRecordsDeserializer deserializer =
        (ContinuousRecordsDeserializer) schema.getTupleDeserializer(relation);
    for (Record kvw : records) {
      deserializer.readRecord(HBaseStorage.FIELDS, kvw);
    }
    val = deserializer.flushRemaining();
    assertEquals("r1", val.getAttribute(ROW));
    assertEquals("v1", val.getAttribute("p1"));
    assertEquals("v2", val.getAttribute("p2"));
    assertEquals("", val.getAttribute("p3"));
  }

  @Test
  public void testNoProp() throws Exception {

    Tuple val = new TupleImpl(relation);
    val.setAttribute(ROW, "r1");

    List<Record> kvws = schema.serialize(val);
    assertEquals(1, kvws.size());

    for (Record kvw : kvws) {
      assertEquals("r1", ByteUtils.toString(kvw.getBytes(HBaseCell.ROWKEY)));
      if (ByteUtils.equals(EMPTY_MARKER, kvw.getBytes(HBaseCell.QUALIFIER))) {
        assertEquals("", ByteUtils.toString(kvw.getBytes(HBaseCell.VALUE)));
      } else {
        fail();
      }
    }
  }
}
