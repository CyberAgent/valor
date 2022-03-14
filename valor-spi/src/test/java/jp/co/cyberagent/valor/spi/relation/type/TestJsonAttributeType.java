package jp.co.cyberagent.valor.spi.relation.type;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class TestJsonAttributeType extends AttributeTypeTestBase<Object> {
  public TestJsonAttributeType() { attrType = JsonAttributeType.INSTANCE; }

  @Test
  public void testToBytesFromBytes() throws IOException {
    Object expected = new HashMap<String, String>() {
      {
        put("one", "1");
        put("two", "2");
        put("three", "3");
      }
    };
    testSerde(expected, toBytes(expected));

    expected = Arrays.asList("one","two","three");
    testSerde(expected, toBytes(expected));
  }

  private byte[] toBytes(Object o) throws IOException, SerdeException {
    ObjectMapper om = new ObjectMapper();
    JsonNode node = om.valueToTree(o);
    ObjectWriter writer = om.writer();
    return writer.writeValueAsBytes(node);
  }
}
