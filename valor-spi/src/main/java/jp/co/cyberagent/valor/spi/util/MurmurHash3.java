package jp.co.cyberagent.valor.spi.util;

public class MurmurHash3 {

  private static final int c1 = 0xcc9e2d51;
  private static final int c2 = 0x1b873593;
  private static final int r1 = 15;
  private static final int r2 = 13;
  private static final int m = 5;
  private static final int n = 0xe6546b64;

  public static int hash(final byte[] value) {
    // default seed
    return hash(value, 0);
  }

  public static int hash(final byte[] value, final int seed) {
    int length = value.length;

    int hash = seed;
    int lastFourByteChunkIndex = (length & 0xfffffffc);

    for (int i = 0; i < lastFourByteChunkIndex; i += 4) {
      // convert 4 byte chunk to int to apply ROL (shift operator)
      int k =
          (value[i] & 0xff) | ((value[i + 1] & 0xff) << 8)
              | ((value[i + 2] & 0xff) << 16) | (value[i + 3] << 24);
      k = k * c1;
      k = rol32(k, r1);
      k = k * c2;
      hash = hash ^ k;
      hash = rol32(hash, r2);
      hash = hash * m + n;
    }

    int remainingByte = 0;

    // CHECKSTYLE:OFF
    switch (length & 0x03) {
      case 3:
        remainingByte |= (value[lastFourByteChunkIndex + 2] & 0xff) << 16;
      case 2:
        remainingByte |= (value[lastFourByteChunkIndex + 1] & 0xff) << 8;
        // FindBugs SF_SWITCH_FALLTHROUGH
      case 1:
        remainingByte |= (value[lastFourByteChunkIndex] & 0xff);
      default:
        // nothing to do
    }
    // CHECKSTYLE:ON

    remainingByte = remainingByte * c1;
    remainingByte = rol32(remainingByte, 15);
    remainingByte = remainingByte * c2;
    hash = hash ^ remainingByte;

    hash = hash ^ length;

    hash = hash ^ (hash >>> 16);
    hash = hash * 0x85ebca6b;
    hash = hash ^ (hash >>> 13);
    hash = hash * 0xc2b2ae35;
    hash = hash ^ (hash >>> 16);
    return hash;
  }

  private static int rol32(final int v, final int r) {
    return (v << r) | (v >>> (32 - r));
  }
}
