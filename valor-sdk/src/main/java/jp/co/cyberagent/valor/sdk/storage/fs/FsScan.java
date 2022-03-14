package jp.co.cyberagent.valor.sdk.storage.fs;

/**
 *
 */
public class FsScan {
  private final byte[] file;

  private final byte[] dir;

  public FsScan(byte[] dir, byte[] file) {
    this.dir = dir;
    this.file = file;
  }

  public byte[] getFile() {
    return file;
  }

  public byte[] getDir() {
    return dir;
  }
}
