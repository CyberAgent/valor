package jp.co.cyberagent.valor.spi.relation.type;

import java.io.IOException;
import org.junit.jupiter.api.Test;

public class TestBooleanAttributeType extends AttributeTypeTestBase<Boolean> {

  public TestBooleanAttributeType() { attrType = BooleanAttributeType.INSTANCE; }

  @Test
  public void testToBytesFromBytes() throws IOException {
    Boolean expected = true;
    testSerde(expected, toBytes(expected));

    expected = false;
    testSerde(expected, toBytes(expected));
  }

  private byte[] toBytes(final boolean b) {
    return new byte[] { (byte) (b ? 1 : 0)};
  }
}
