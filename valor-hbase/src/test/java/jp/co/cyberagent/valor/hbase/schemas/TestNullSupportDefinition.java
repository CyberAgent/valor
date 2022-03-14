package jp.co.cyberagent.valor.hbase.schemas;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedMonoKeyValueSchema;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.Storage;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableUtils;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
public class TestNullSupportDefinition {

  private static final String KEY = "key";

  private static final String VALUE = "val";

  private static final String FAMILY = "fam";

  private static final String QUALIFIER = "qual";

  private Schema schema;

  private Relation relation;

  private String tableName;

  public TestNullSupportDefinition() throws Exception {
    relation = ImmutableRelation.builder().relationId("null_support")
        .addAttribute(KEY, true, StringAttributeType.INSTANCE)
        .addAttribute(VALUE, false, StringAttributeType.INSTANCE)
        .build();

    tableName = "testTable";
    SchemaDescriptor descriptor =
        ImmutableSchemaDescriptor.builder().schemaId("test")
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(new ValorConfImpl())
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create(tableName)))
            .addField(HBaseCell.ROWKEY, Arrays.asList(AttributeValueFormatter.create(KEY)))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create(FAMILY)))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(ConstantFormatter.create(QUALIFIER)))
            .addField(HBaseCell.VALUE,
                Arrays.asList(VintSizePrefixHolder.create(AttributeValueFormatter.create(VALUE))))
            .build();
    Storage storage = new HBaseStorage(new ValorConfImpl());
    schema = storage.buildSchema(relation, descriptor);
  }

  @Test
  public void testValidSchema() {
    assertTrue(schema instanceof SortedMonoKeyValueSchema);
  }

  @Test
  public void testSingleKeyParam() throws Exception {

    Tuple val = new TupleImpl(relation);
    val.setAttribute(KEY, "k");
    val.setAttribute(VALUE, null);

    List<Record> records = schema.serialize(val);
    assertEquals(1, records.size());
    Record record = records.get(0);
    assertArrayEquals(Bytes.toBytes("k"), record.getBytes(HBaseCell.ROWKEY));

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    WritableUtils.writeVInt(new DataOutputStream(bos), -1);
    assertArrayEquals(bos.toByteArray(), record.getBytes(HBaseCell.VALUE));

    TupleDeserializer deserializer = schema.getTupleDeserializer(relation);
    deserializer.readRecord(HBaseStorage.FIELDS, records.get(0));
    Tuple deserialized = deserializer.pollTuple();
    assertEquals("k", deserialized.getAttribute(KEY));
    assertNull(deserialized.getAttribute(VALUE));

    val.setAttribute(VALUE, "nonnull");
    records = schema.serialize(val);
    assertEquals(1, records.size());
    record = records.get(0);
    assertArrayEquals(Bytes.toBytes("k"), record.getBytes(HBaseCell.ROWKEY));
    byte[] bytesValue = Bytes.toBytes("nonnull");
    bos = new ByteArrayOutputStream();
    Bytes.writeByteArray(new DataOutputStream(bos), bytesValue);
    assertArrayEquals(bos.toByteArray(), record.getBytes(HBaseCell.VALUE));

    deserializer = schema.getTupleDeserializer(relation);
    deserializer.readRecord(HBaseStorage.FIELDS, records.get(0));
    deserialized = deserializer.pollTuple();
    assertEquals("k", deserialized.getAttribute(KEY));
    assertEquals("nonnull", deserialized.getAttribute(VALUE));
  }
}
