package jp.co.cyberagent.valor.sdk;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

public class BytesDataInputStream extends DataInputStream {

  public BytesDataInputStream(byte[] input) {
    super(new ByteArrayInputStream(input));
  }
}
