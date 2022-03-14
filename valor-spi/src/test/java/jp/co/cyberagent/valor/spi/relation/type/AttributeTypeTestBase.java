package jp.co.cyberagent.valor.spi.relation.type;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class AttributeTypeTestBase<T> {

  protected AttributeType<T> attrType;

  @Test
  public void testNull() throws Exception {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream dos = new DataOutputStream(baos)) {
      int l = attrType.write(dos, null);
      assertTrue(l < 0);
    }

    T val = attrType.read(new byte[0], 0, -1);
  }

  protected void testSerde(Object obj, byte[] bytes) throws IOException {
    byte[] actual = null;
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos =
        new DataOutputStream(baos)) {
      attrType.write(dos, obj);
      actual = baos.toByteArray();
      assertArrayEquals(bytes, actual,
          ByteUtils.toStringBinary(bytes) + " <> " + ByteUtils.toStringBinary(actual));
    }

    Object deserialized = attrType.read(bytes, 0, bytes.length);
    assertEquals(obj, deserialized);
  }
}
