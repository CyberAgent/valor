package jp.co.cyberagent.valor.spi.relation.type;

import java.io.IOException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestDoubleAttributeType extends AttributeTypeTestBase<Double> {

  public TestDoubleAttributeType() {
    attrType = DoubleAttributeType.INSTANCE;
  }

  @Test
  public void testToBytesFromBytes() throws IOException {
    Double expected = 0.1;
    testSerde(expected, ByteUtils.toBytes(expected));
  }
}
