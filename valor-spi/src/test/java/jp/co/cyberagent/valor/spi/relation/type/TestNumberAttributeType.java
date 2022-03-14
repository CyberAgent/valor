package jp.co.cyberagent.valor.spi.relation.type;

import java.io.IOException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestNumberAttributeType extends AttributeTypeTestBase<Number> {

  public TestNumberAttributeType() {
    attrType = NumberAttributeType.INSTANCE;
  }

  @Test
  public void testLong() throws IOException {
    long expectedLong = 123l;
    testSerde(expectedLong, ByteUtils.toBytes(expectedLong));
  }

  @Test
  public void testFloat() throws IOException {
    float expectedFloat = 4.56f;
    testSerde(expectedFloat, ByteUtils.toBytes(expectedFloat));
  }
}
