package jp.co.cyberagent.valor.spi.relation.type;

import java.io.IOException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestFloatAttributeType extends AttributeTypeTestBase<Float> {

  public TestFloatAttributeType() {
    attrType = FloatAttributeType.INSTANCE;
  }

  @Test
  public void testToBytesFromBytes() throws IOException {
    float expected = 0.1f;
    testSerde(expected, ByteUtils.toBytes(expected));
  }
}
