package jp.co.cyberagent.valor.spi.relation.type;

import java.io.IOException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestLongAttributeType extends AttributeTypeTestBase<Long> {

  public TestLongAttributeType() {
    attrType = LongAttributeType.INSTANCE;
  }

  @Test
  public void testToBytesFromBytes() throws IOException {
    long expected = 123l;
    testSerde(expected, ByteUtils.toBytes(expected));
  }
}
