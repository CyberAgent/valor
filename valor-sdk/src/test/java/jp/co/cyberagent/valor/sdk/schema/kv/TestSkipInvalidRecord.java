package jp.co.cyberagent.valor.sdk.schema.kv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
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
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScanner;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;
import org.junit.jupiter.api.Test;

public class TestSkipInvalidRecord {

  private static final String KEY_ATTR1 = "keyAttr1";
  private static final String KEY_ATTR2 = "keyAttr2";
  private static final String VAL_ATTR = "valAttr";
  private final Relation relation;
  private Schema schema;
  private EmbeddedEKVStorage storage;

  public TestSkipInvalidRecord() throws Exception {
    ValorConf conf = new ValorConfImpl();

    relation = ImmutableRelation.builder().relationId("mmr")
        .addAttribute(KEY_ATTR1, true, StringAttributeType.INSTANCE)
        .addAttribute(KEY_ATTR2, true, StringAttributeType.INSTANCE)
        .addAttribute(VAL_ATTR, false, StringAttributeType.INSTANCE).build();

    AttributeValueFormatter.Factory formatterFactory = new AttributeValueFormatter.Factory();
    AttributeValueFormatter key1Formatter = formatterFactory.create(KEY_ATTR1);
    AttributeValueFormatter key2Formatter = formatterFactory.create(KEY_ATTR2);
    AttributeValueFormatter valFormatter = formatterFactory.create(VAL_ATTR);

    SchemaDescriptor schemaDescriptor =
        ImmutableSchemaDescriptor.builder().schemaId("test")
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(
                    new VintSizePrefixHolder().setFormatter(key1Formatter),
                    new VintSizePrefixHolder().setFormatter(key2Formatter)))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter("\u0001")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList(valFormatter))
            .build();
    storage = new EmbeddedEKVStorage(conf);
    schema = storage.buildSchema(relation, schemaDescriptor);
  }

  @SuppressWarnings(value = "unchecked")
  @Test
  public void testLackedAttribute() throws Exception {
    ValorConf conf = new ValorConfImpl();
    conf.set(ValorConf.IGNORE_INVALID_RECORD.name, "true");

    storage.put(new byte[] {0x01, 0x58}, new byte[] {0x01}, new byte[] {0x02});

    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(KEY_ATTR1, "keyAttr1");
    tuple.setAttribute(KEY_ATTR2, "keyAttr2");
    tuple.setAttribute(VAL_ATTR, "value");

    List<Record> records = schema.serialize(tuple);
    assertEquals(1, records.size());

    try (StorageConnection conn = schema.getConnectionFactory().connect()) {
      StorageMutation mutation = schema.buildInsertMutation(tuple);
      mutation.execute(conn);
      SchemaScan scan = schema.buildScan(relation.getAttributeNames(), null);
      scan.setConf(conf);
      try (SchemaScanner schemaScanner = schema.getScanner(scan, conn)) {
        Tuple scanned = schemaScanner.next();
        assertThat(scanned.getAttribute(KEY_ATTR1), equalTo("keyAttr1"));
        assertThat(scanned.getAttribute(KEY_ATTR2), equalTo("keyAttr2"));
        assertThat(scanned.getAttribute(VAL_ATTR), equalTo("value"));
        assertThat(schemaScanner.next(), is(nullValue()));
      }
    }
  }
}
