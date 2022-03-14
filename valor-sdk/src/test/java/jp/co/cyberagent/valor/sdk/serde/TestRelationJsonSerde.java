package jp.co.cyberagent.valor.sdk.serde;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.ArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestRelationJsonSerde {

  static final ObjectMapper om = new ObjectMapper();
  static MetadataJsonSerde serde;

  @BeforeAll
  public static void init() {
    ValorContext context = StandardContextFactory.create();
    serde = new MetadataJsonSerde(context);
  }

  @ParameterizedTest
  @ValueSource(strings = {
      "relation/testAttributeTypes.json",
      "relation/testWithSchemas.json"
  })
  public void test(String relationExp) throws Exception {
    final String relId = "test";
    Map<String, Object> m;
    try (InputStream is = ClassLoader.getSystemResourceAsStream(relationExp)) {
      m = om.readValue(is, Map.class);
    }
    m.put(MetadataJsonSerde.RELATION_ID, relId);
    Relation o;
    try (InputStream is = ClassLoader.getSystemResourceAsStream(relationExp)) {
      o = serde.readRelation(relId, is);
    }
    assertRelation(o, m);
    // check schema if exists
    if (m.containsKey(MetadataJsonSerde.SCHEMAS)) {
      List<Map<String, Object>> s = (List<Map<String, Object>>) m.get(MetadataJsonSerde.SCHEMAS);
      List<SchemaDescriptor> schemas = o.getSchemas();
      assertEquals(s.size(), schemas.size());
      // TODO assert schema equals
    }
    // test serialization (relation only)
    byte[] json = serde.serialize(o);
    o = serde.readRelation(relId, json);
    assertRelation(o, m);
  }

  private void assertRelation(Relation o, Map<String, Object> m) {
    assertThat(o.getRelationId(), equalTo(m.get(MetadataJsonSerde.RELATION_ID)));
    List<Map<String, Object>> attrMaps =
        (List<Map<String, Object>>) m.get(MetadataJsonSerde.ATTRIBUTES);
    for (int attrIndex = 0; attrIndex < o.getAttributeNames().size(); attrIndex++) {
      String attrName = o.getAttributeNames().get(attrIndex);
      Relation.Attribute a = o.getAttribute(attrName);
      Map<String, Object> attrMap = attrMaps.get(attrIndex);
      assertEquals(attrName, attrMap.get(MetadataJsonSerde.ATTR_NAME));
      assertEquals(o.isKey(attrName), attrMap.get(MetadataJsonSerde.ATTR_IS_KEY));
      assertType(a.type(), attrMap.get(MetadataJsonSerde.ATTR_TYPE));
      assertEquals(a.isNullable(), attrMap.getOrDefault(MetadataJsonSerde.ATTR_IS_NULLABLE, false));
    }
  }

  private void assertType(AttributeType a, Object e) {
    if (e instanceof String) {
      AttributeType expectedType = AttributeType.getPrimitiveType((String) e);
      assertEquals(expectedType, a);
    } else if (e instanceof Map) {
      Map<String, Object> m = (Map<String, Object>) e;
      String typeName = (String) m.get(MetadataJsonSerde.TYPE_NAME);
      if (ArrayAttributeType.NAME.equals(typeName)) {
        assertTrue(a instanceof ArrayAttributeType);
        ArrayAttributeType o = (ArrayAttributeType) a;
        assertType(o.getElementType(), m.get(MetadataJsonSerde.ARRAY_ELEMENT_TYPE));
      } else if (MapAttributeType.NAME.equals(typeName)) {
        assertTrue(a instanceof MapAttributeType);
        MapAttributeType o = (MapAttributeType) a;
        assertType(o.getKeyType(), m.get(MetadataJsonSerde.MAP_KEY_TYPE));
        assertType(o.getValueType(), m.get(MetadataJsonSerde.MAP_VALUE_TYPE));
        // TODO support more complex types
      } else {
        assertType(a, typeName);
      }
    }
  }
}
