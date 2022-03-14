package jp.co.cyberagent.valor.spi.util;

import static org.junit.jupiter.api.Assertions.assertEquals;


import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestByteUtil {

  @ParameterizedTest
  @ValueSource(ints = {1, 225})
  public void testReadVint(int expected) throws Exception {

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream dos = new DataOutputStream(baos)
    ) {
      ByteUtils.writeVInt(dos, expected);
      byte[] buf = baos.toByteArray();
      int actual = ByteUtils.readVInt(buf, 0);
      assertEquals(expected, actual);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 225})
  public void testReadVintWithOffset(int expected) throws Exception {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         DataOutputStream dos = new DataOutputStream(baos)
    ) {
      ByteUtils.writeVInt(dos, expected);
      byte[] buf = baos.toByteArray();
      buf = ByteUtils.add(new byte[]{0x00}, buf);
      int actual = ByteUtils.readVInt(buf, 1);
      assertEquals(expected, actual);
    }
  }

}
