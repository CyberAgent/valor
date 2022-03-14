package jp.co.cyberagent.valor.spi.relation.type;

import java.io.IOException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestIntegerAttributeType extends AttributeTypeTestBase<Integer> {

  public TestIntegerAttributeType() {
    attrType = IntegerAttributeType.INSTANCE;
  }

  @Test
  public void testToBytesFromBytes() throws IOException {
    int expected = 123;
    testSerde(expected, ByteUtils.toBytes(expected));
  }
}
