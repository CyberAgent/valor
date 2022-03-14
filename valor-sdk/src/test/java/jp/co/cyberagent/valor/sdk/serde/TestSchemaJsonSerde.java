package jp.co.cyberagent.valor.sdk.serde;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.SingletonStubPlugin;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


public class TestSchemaJsonSerde {

  static final ObjectMapper om = new ObjectMapper();

  private final MetadataJsonSerde serde;

  public TestSchemaJsonSerde() {
    ValorContext context = StandardContextFactory.create();
    context.installPlugin(new SingletonStubPlugin());
    serde = new MetadataJsonSerde(context);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "schema/testDecoratedSchema.json", // for backward compatibility
      "schema/testMapPropValueSchema.json"})
  public void test(String schemaExp) throws Exception {
    Map<String, Object> m;
    try (InputStream is = ClassLoader.getSystemResourceAsStream(schemaExp)) {
      m = om.readValue(is, Map.class);
    }
    final String schemaId = "s";
    m.put(MetadataJsonSerde.SCHEMA_ID, schemaId);
    SchemaDescriptor o;
    try (InputStream is = ClassLoader.getSystemResourceAsStream(schemaExp)) {
      o = serde.readSchema(schemaId, is);
    }
    assertSchema(o, m);
    // test serialization
    byte[] json = serde.serialize(o);
    o = serde.readSchema(schemaId, json);
    assertSchema(o, m);
  }

  private void assertSchema(SchemaDescriptor o, Map<String, Object> m) {
    assertThat(o.getSchemaId(), equalTo(m.get(MetadataJsonSerde.SCHEMA_ID)));
    if (m.containsKey(MetadataJsonSerde.PRIMARY)) {
      assertThat(o.isPrimary(), equalTo(m.get(MetadataJsonSerde.PRIMARY)));
    }
    if (m.containsKey(MetadataJsonSerde.MODE)) {
      assertThat(o.getMode().name(), equalTo(m.get(MetadataJsonSerde.MODE)));
    }

    Map<String, Object> storageMap = (Map<String, Object>) m.get(MetadataJsonSerde.STORAGE);
    assertThat(o.getStorageClassName(), equalTo(storageMap.get(MetadataJsonSerde.CLASS)));
    // TODO check storage config

    List<FieldLayout> fields = o.getFields();
    List<Map<String, Object>> fieldMaps =
        (List<Map<String, Object>>) m.get(MetadataJsonSerde.FIELDS);
    for (int fieldIndex = 0; fieldIndex < fields.size(); fieldIndex++) {
      FieldLayout field = fields.get(fieldIndex);
      assertLayout(field, fieldMaps.get(fieldIndex));
    }
  }

  private void assertLayout(FieldLayout o, Map<String, Object> m) {
    List<Map<String, Object>> fm = (List<Map<String, Object>>) m.get(MetadataJsonSerde.FORMAT);
    assertEquals(o.getFormatters().size(), fm.size());
    for (int formatterIndex = 0; formatterIndex < fm.size(); formatterIndex++) {
      assertFormatter(o.getFormatters().get(formatterIndex), fm.get(formatterIndex));
    }
  }

  private void assertFormatter(Segment o, Map<String, Object> m) {
    String type = o.getName();
    if (type == null) {
      type = o.getClass().getCanonicalName();
    }
    if (o instanceof Holder) {
      assertEquals(m.get(MetadataJsonSerde.FORMATTER_TYPE), type);
      assertEquals(m.get(MetadataJsonSerde.PROPS), o.getProperties());
      Object v = m.get(MetadataJsonSerde.DECORATEE);
      if (v == null) {
        v = m.get(MetadataJsonSerde.VALUE);
      }
      assertFormatter(o.getFormatter(), (Map<String, Object>) v);
    } else {
      assertEquals(m.get(MetadataJsonSerde.FORMATTER_TYPE), type);
      assertEquals(m.get(MetadataJsonSerde.PROPS), o.getProperties());
    }
  }
}
