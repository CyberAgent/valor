package jp.co.cyberagent.valor.sdk;

import org.mockito.ArgumentMatcher;

public class EqualBytes implements ArgumentMatcher<byte[]> {

  private final byte[] target;

  public EqualBytes(byte[] target) {
    this.target = target;
  }

  public static EqualBytes equalBytes(byte[] target) {
    return new EqualBytes(target);
  }

  @Override
  public boolean matches(byte[] o) {
    if (!(o instanceof byte[])) {
      return false;
    }
    byte[] otherBytes = (byte[]) o;
    if (target.length != otherBytes.length) {
      return false;
    }
    for (int i = 0; i < target.length; i++) {
      if (target[i] != otherBytes[i]) {
        return false;
      }
    }
    return true;
  }


}
