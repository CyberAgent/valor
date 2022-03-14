package jp.co.cyberagent.valor.spi.util;

import java.io.Closeable;
import java.io.IOException;

public class Closer {
  private IOException ioe;

  public void close(Closeable closeable) {
    try {
      closeable.close();
    } catch (IOException e) {
      e.printStackTrace();
      if (ioe != null) {
        ioe = e;
      }
    }
  }

  public void throwIfFailed() throws IOException {
    if (ioe != null) {
      throw ioe;
    }
  }
}
