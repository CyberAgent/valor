package jp.co.cyberagent.valor.spi.relation.type;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

@SuppressWarnings( {"unchecked"})
public class TestStringAttributeType extends AttributeTypeTestBase<String> {

  public TestStringAttributeType() {
    attrType = StringAttributeType.INSTANCE;
  }

  @Test
  public void testToBytesFromBytes() throws IOException {
    String expectedString = "string";
    testSerde(expectedString, expectedString.getBytes(StandardCharsets.UTF_8));
  }
}
