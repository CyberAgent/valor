package jp.co.cyberagent.valor.spi.storage;

import java.io.Closeable;
import java.io.IOException;

public abstract class StorageScanner implements Closeable {

  public abstract Record next() throws IOException;
}
