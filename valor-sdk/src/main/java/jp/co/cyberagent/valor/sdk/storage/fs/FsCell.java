package jp.co.cyberagent.valor.sdk.storage.fs;

import jp.co.cyberagent.valor.spi.exception.InvalidFieldException;
import jp.co.cyberagent.valor.spi.storage.Record;

/**
 *
 */
public class FsCell implements Record {
  public static final String DIR = "dir";
  public static final String FILE = "file";
  public static final String VALUE = "value";

  private byte[] dir;
  private byte[] file;
  private byte[] value;

  public FsCell() {
  }

  public FsCell(byte[] dir, byte[] file, byte[] value) {
    this.dir = dir;
    this.file = file;
    this.value = value;
  }

  @Override
  public byte[] getBytes(String fieldName) throws InvalidFieldException {
    if (DIR.equalsIgnoreCase(fieldName)) {
      return dir;
    } else if (FILE.equalsIgnoreCase(fieldName)) {
      return file;
    } else if (VALUE.equalsIgnoreCase(fieldName)) {
      return value;
    }
    throw new InvalidFieldException(fieldName, "fs");
  }

  @Override
  public void setBytes(String fieldName, byte[] v) throws InvalidFieldException {
    if (DIR.equalsIgnoreCase(fieldName)) {
      dir = v;
    } else if (FILE.equalsIgnoreCase(fieldName)) {
      file = v;
    } else if (VALUE.equalsIgnoreCase(fieldName)) {
      value = v;
    }
    throw new InvalidFieldException(fieldName, "fs");
  }
}
