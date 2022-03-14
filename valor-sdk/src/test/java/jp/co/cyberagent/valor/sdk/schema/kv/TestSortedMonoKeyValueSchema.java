package jp.co.cyberagent.valor.sdk.schema.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.NumberAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestSortedMonoKeyValueSchema {

  private static final String KEY_ATTR = "keyAttr";
  private static final String VAL_ATTR = "valAttr";
  private final Relation relation;
  private Schema schema;
  private EmbeddedEKVStorage storage;

  public TestSortedMonoKeyValueSchema() throws Exception {
    ValorConf conf = new ValorConfImpl();

    relation = ImmutableRelation.builder().relationId("mmr").addAttribute(KEY_ATTR, true,
        StringAttributeType.INSTANCE).addAttribute(VAL_ATTR, false, NumberAttributeType.INSTANCE)
        .build();

    SchemaDescriptor schemaDescriptor =
        ImmutableSchemaDescriptor.builder()
            .schemaId("test")
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(new AttributeValueFormatter(KEY_ATTR)))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter("")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter(VAL_ATTR)))
            .build();
    storage = new EmbeddedEKVStorage(conf);
    schema = storage.buildSchema(relation, schemaDescriptor);
  }

  @SuppressWarnings(value = "unchecked")
  @Test
  public void testInsertAndDelete() throws Exception {

    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(KEY_ATTR, "keyAttr1");
    tuple.setAttribute(VAL_ATTR, 100l);

    List<Record> records = schema.serialize(tuple);
    assertEquals(1, records.size());

    try (StorageConnection conn = schema.getConnectionFactory().connect()) {
      StorageMutation insert = schema.buildInsertMutation(tuple);
      insert.execute(conn);
      assertEquals(1, storage.get(ByteUtils.toBytes("keyAttr1")).size());

      StorageMutation delete = schema.buildDeleteMutation(tuple);
      delete.execute(conn);
      assertEquals(0, storage.get(ByteUtils.toBytes("keyAttr1")).size());
    }
  }
}
