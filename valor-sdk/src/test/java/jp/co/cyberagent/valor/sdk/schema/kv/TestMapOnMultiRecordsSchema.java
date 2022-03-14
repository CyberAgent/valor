package jp.co.cyberagent.valor.sdk.schema.kv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MapKeyFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MapValueFormatter;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScanner;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;
import jp.co.cyberagent.valor.spi.util.ByteTestUtil;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestMapOnMultiRecordsSchema {

  private static final String KEY_ATTR = "keyAttr";
  private static final String VAL_ATTR = "valAttr";
  private final Relation relation;
  private Schema schema;
  private Storage storage;

  public TestMapOnMultiRecordsSchema() throws Exception {
    ValorConf conf = new ValorConfImpl();

    // define map<string, string> type
    MapAttributeType mapType = new MapAttributeType();
    mapType.addGenericElementType(StringAttributeType.INSTANCE);
    mapType.addGenericElementType(StringAttributeType.INSTANCE);
    relation = ImmutableRelation.builder().relationId("mmr").addAttribute(KEY_ATTR, true,
        StringAttributeType.INSTANCE).addAttribute(VAL_ATTR, false, mapType).build();

    SchemaDescriptor schemaDescriptor =
        ImmutableSchemaDescriptor.builder().schemaId("test")
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(AttributeValueFormatter.create(KEY_ATTR)))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(MapKeyFormatter.create(VAL_ATTR)))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList(MapValueFormatter.create(VAL_ATTR)))
            .build();
    storage = new EmbeddedEKVStorage(conf);
    schema = storage.buildSchema(relation, schemaDescriptor);
  }

  @SuppressWarnings(value = "unchecked")
  @Test
  public void testMultiProps() throws Exception {

    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(KEY_ATTR, "keyAttr1");
    tuple.setAttribute(VAL_ATTR, new HashMap<String, String>() {{
      put("k1", "v1");
      put("k2", "v2");
    }});

    List<Record> records = schema.serialize(tuple);
    assertEquals(2, records.size());

    Record record = records.get(0);
    ByteTestUtil.assertBytesEquals(ByteUtils.toBytes("keyAttr1"),
        record.getBytes(EmbeddedEKVStorage.KEY));
    ByteTestUtil.assertBytesEquals(ByteUtils.toBytes("k1"), record.getBytes(EmbeddedEKVStorage.COL));
    ByteTestUtil.assertBytesEquals(ByteUtils.toBytes("v1"), record.getBytes(EmbeddedEKVStorage.VAL));
    record = records.get(1);
    ByteTestUtil.assertBytesEquals(ByteUtils.toBytes("keyAttr1"),
        record.getBytes(EmbeddedEKVStorage.KEY));
    ByteTestUtil.assertBytesEquals(ByteUtils.toBytes("k2"), record.getBytes(EmbeddedEKVStorage.COL));
    ByteTestUtil.assertBytesEquals(ByteUtils.toBytes("v2"), record.getBytes(EmbeddedEKVStorage.VAL));

    try (StorageConnection conn = schema.getConnectionFactory().connect()) {
      StorageMutation mutation = schema.buildInsertMutation(tuple);
      mutation.execute(conn);
      SchemaScan scan = schema.buildScan(relation.getAttributeNames(), null);
      try (SchemaScanner schemaScanner = schema.getScanner(scan, conn)) {
        Tuple scanned = schemaScanner.next();
        assertThat(scanned.getAttribute(KEY_ATTR), equalTo("keyAttr1"));
        assertThat((Map<String, String>) scanned.getAttribute(VAL_ATTR), hasEntry(equalTo("k1"),
            equalTo("v1")));
        assertThat((Map<String, String>) scanned.getAttribute(VAL_ATTR), hasEntry(equalTo("k2"),
            equalTo("v2")));
        assertThat(schemaScanner.next(), is(nullValue()));
      }
    }
  }
}
