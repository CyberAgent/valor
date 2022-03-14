package jp.co.cyberagent.valor.spi.relation.type;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestByteArrayAttributeType extends AttributeTypeTestBase<Bytes> {

  public TestByteArrayAttributeType() {
    attrType = ByteArrayAttributeType.INSTANCE;
  }

  @Test
  public void testToBytesFromBytes() throws IOException {
    byte[] value = new byte[] {0x01, 0x02};
    testArraySerde(new Bytes(value), value);
  }

  protected void testArraySerde(Bytes obj, byte[] bytes) throws IOException {
    byte[] actual = null;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos =
        new DataOutputStream(baos)) {
      attrType.write(dos, obj);
      actual = baos.toByteArray();
      assertArrayEquals(bytes, actual,
          ByteUtils.toStringBinary(bytes) + " <> " + ByteUtils.toStringBinary(actual));
    }

    Bytes deserialized = attrType.read(bytes, 0, bytes.length);
    assertEquals(new Bytes(obj), deserialized);
  }
}
