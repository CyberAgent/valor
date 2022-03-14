package jp.co.cyberagent.valor.sdk.storage.fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

/**
 *
 */
public class FsScanner extends StorageScanner {
  private BufferedReader reader;
  private StorageScan scan;
  private byte[] currentDir;
  private byte[] currentFile;

  public FsScanner(StorageScan scan) {
    this.scan = scan;
  }

  @Override
  public Record next() throws IOException {
    if (reader == null) {
      init();
    }
    String line = reader.readLine();
    return line == null ? null : new FsCell(currentDir, currentFile, line.getBytes());
  }

  private void init() throws IOException {
    // TODO scan multiple directory/files
    currentDir = scan.getStart(FsCell.DIR);
    currentFile = scan.getStart(FsCell.FILE);
    File f = new File(ByteUtils.toString(currentDir), ByteUtils.toString(currentFile));
    reader = new BufferedReader(new FileReader(f));
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
    }
  }
}
