package jp.co.cyberagent.valor.sdk.schema.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MapKeyFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MapValueFormatter;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.serde.ContinuousRecordsDeserializer;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestMultiKVMapSchema {

  private static final String KEY_ATTR = "keyAttr";
  private static final String VAL_ATTR = "valAttr";

  private Relation relation;
  private Schema schema;
  private EmbeddedEKVStorage storage;

  public TestMultiKVMapSchema() throws Exception {
    ValorConf conf = new ValorConfImpl();

    MapAttributeType mapType = new MapAttributeType();
    mapType.addGenericElementType(StringAttributeType.INSTANCE);
    mapType.addGenericElementType(LongAttributeType.INSTANCE);
    relation= ImmutableRelation.builder().relationId("mkvm")
        .addAttribute(KEY_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(VAL_ATTR, false, mapType)
        .build();

    SchemaDescriptor schemaDescriptor = ImmutableSchemaDescriptor.builder()
        .schemaId("test")
        .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
        .storageConf(conf)
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(new AttributeValueFormatter(KEY_ATTR)))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(
            VintSizePrefixHolder.create(new MapValueFormatter(VAL_ATTR)),
            new MapKeyFormatter(VAL_ATTR)))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new MapValueFormatter(VAL_ATTR)))
        .build();

    storage = new EmbeddedEKVStorage(conf);
    schema = storage.buildSchema(relation, schemaDescriptor);
  }

  @Test
  public void testDifferentValues() throws Exception {

    Tuple tuple = new TupleImpl(this.relation);
    tuple.setAttribute(KEY_ATTR, "r1");
    tuple.setAttribute(VAL_ATTR, new HashMap<String,Long>(){{
      put("k1", 1l);
      put("k2", 2l);
    }});

    List<Record> records = schema.serialize(tuple);
    assertEquals(2, records.size());

    byte[] expectedPrefix = ByteUtils.vintToBytes(8);
    for (Record record : records) {
      assertEquals("r1", ByteUtils.toString(record.getBytes(EmbeddedEKVStorage.KEY)));
      byte[] col = record.getBytes(EmbeddedEKVStorage.COL);
      byte[] val = record.getBytes(EmbeddedEKVStorage.VAL);
      if(ByteUtils.equals(col, ByteUtils.add(expectedPrefix, ByteUtils.toBytes(1l), ByteUtils.toBytes("k1")))) {
        assertEquals(1l, ByteUtils.toLong(val));
      } else if(ByteUtils.equals(col, ByteUtils.add(expectedPrefix, ByteUtils.toBytes(2l), ByteUtils.toBytes("k2")))) {
        assertEquals(2l, ByteUtils.toLong(val));
      } else {
        fail();
      }
    }
  }

  @Test
  public void testSameValues() throws Exception {

    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(KEY_ATTR, "r1");
    tuple.setAttribute(VAL_ATTR, new HashMap<String,Long>(){{
      put("k1", 1l);
      put("k2", 1l);
    }});

    List<Record> records = schema.serialize(tuple);
    assertEquals(2, records.size());

    byte[] expectedPrefix = ByteUtils.vintToBytes(8);
    ContinuousRecordsDeserializer deserializer =
        (ContinuousRecordsDeserializer) schema.getTupleDeserializer(relation);
    for (Record record : records) {
      assertEquals("r1", ByteUtils.toString(record.getBytes(EmbeddedEKVStorage.KEY)));
      byte[] col = record.getBytes(EmbeddedEKVStorage.COL);
      byte[] val = record.getBytes(EmbeddedEKVStorage.VAL);
      if(ByteUtils.equals(col, ByteUtils.add(expectedPrefix, ByteUtils.toBytes(1l), ByteUtils.toBytes("k1")))) {
        assertEquals(1l, ByteUtils.toLong(val));
      } else if(ByteUtils.equals(col, ByteUtils.add(expectedPrefix, ByteUtils.toBytes(1l), ByteUtils.toBytes("k2")))) {
        assertEquals(1l, ByteUtils.toLong(val));
      } else {
        fail();
      }
      deserializer.readRecord(EmbeddedEKVStorage.FIELDS, record);
    }
    Tuple t = deserializer.flushRemaining();
    Map m = (Map) t.getAttribute(VAL_ATTR);
    assertEquals(1l, m.get("k1"));
    assertEquals(1l, m.get("k2"));
  }

  @Test
  public void testEmpty() throws Exception {

    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(KEY_ATTR, "r1");
    tuple.setAttribute(VAL_ATTR, new HashMap<String,Long>());

    List<Record> records = schema.serialize(tuple);
    assertEquals(1, records.size());

    ContinuousRecordsDeserializer deserializer =
        (ContinuousRecordsDeserializer) schema.getTupleDeserializer(relation);
    deserializer.readRecord(EmbeddedEKVStorage.FIELDS, records.get(0));

    Tuple t = deserializer.flushRemaining();
    Map m = (Map) t.getAttribute(VAL_ATTR);
    assertEquals(0, m.size());
  }

}
