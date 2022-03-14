package jp.co.cyberagent.valor.spi.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/**
 *
 */
public class ByteTestUtil {

  public static void assertBytesEquals(byte[] expected, byte[] actual) {
    assertArrayEquals(expected, actual,
        ByteUtils.toStringBinary(expected) + " expected but " + ByteUtils.toStringBinary(actual));
  }
}
