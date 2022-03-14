package jp.co.cyberagent.valor.hbase.storage.snapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.HFileLink;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotReferenceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SnapshotScanner extends StorageScanner {

  static Logger LOG = LoggerFactory.getLogger(SnapshotScanner.class);

  private final String snapshot;
  private final Configuration conf;

  private transient HFileScanner scanner;
  private transient Iterator<Path> fileIterator;
  private TableName table;
  private FileSystem fs;

  public SnapshotScanner(String snapshot, Configuration conf) throws IOException {
    this.snapshot = snapshot;
    this.conf = conf;
    init();
  }

  private void init() throws IOException {
    Path rootDir = new Path(conf.get("hbase.rootdir"));
    fs = rootDir.getFileSystem(conf);
    rootDir = rootDir.makeQualified(fs.getUri(), fs.getWorkingDirectory());
    Path snapshotDir = new Path(new Path(rootDir, ".hbase-snapshot"), snapshot);
    HBaseProtos.SnapshotDescription
        snapshotDesc = SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);
    table = TableName.valueOf(snapshotDesc.getTable());
    // Get snapshot files
    List<Path> files = new ArrayList<>();
    SnapshotReferenceUtil.visitReferencedFiles(conf, fs, snapshotDir, snapshotDesc,
        (regionInfo, family, storeFile) -> {
          if (storeFile.hasReference()) {
            // copied as part of the manifest
          } else {
            String region = regionInfo.getEncodedName();
            String hfile = storeFile.getName();
            Path path = HFileLink.createPath(table, region, family, hfile);

            SnapshotProtos.SnapshotFileInfo fileInfo =
                SnapshotProtos.SnapshotFileInfo.newBuilder()
                    .setType(SnapshotProtos.SnapshotFileInfo.Type.HFILE)
                    .setHfile(path.toString())
                    .build();
            Path p = getHFilePath(fs, fileInfo);
            LOG.info("add {} to input splits", p);
            files.add(p);
          }
        });
    fileIterator = files.iterator();
  }

  private Path getHFilePath(FileSystem fs, final SnapshotProtos.SnapshotFileInfo fileInfo)
      throws IOException {
    Configuration conf = fs.getConf();
    switch (fileInfo.getType()) {
      case HFILE:
        Path inputPath = new Path(fileInfo.getHfile());
        HFileLink link = HFileLink.buildFromHFileLinkPattern(conf, inputPath);
        return link.getAvailablePath(fs);
      default:
        throw new IOException("Invalid File Type: " + fileInfo.getType().toString());
    }
  }


  @Override
  public Record next() throws IOException {
    if (scanner == null) {
      if (!moveToNextScanner()) {
        return null;
      }
    }
    do {
      if (scanner.getKeyValue() != null) {
        Cell cell = scanner.getKeyValue();
        scanner.next();
        return new HBaseCell(table, cell);
      }
    } while (moveToNextScanner());
    return null;
  }

  private boolean moveToNextScanner() throws IOException {
    if (!fileIterator.hasNext()) {
      return false;
    }
    Path path = fileIterator.next();
    this.scanner =
        HFile.createReader(fs, path, new CacheConfig(conf), conf).getScanner(true, false, false);
    this.scanner.seekTo();
    return true;
  }

  @Override
  public void close() throws IOException {
    try {
      if (scanner != null) {
        scanner.close();
      }
    } finally {
      fs.close();
    }
  }

}
