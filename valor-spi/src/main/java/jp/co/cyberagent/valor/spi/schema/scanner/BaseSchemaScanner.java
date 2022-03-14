package jp.co.cyberagent.valor.spi.schema.scanner;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseSchemaScanner implements SchemaScanner {
  static final Logger LOG = LoggerFactory.getLogger(BaseSchemaScanner.class);

  protected final StorageConnection conn;
  protected final Predicate<Tuple> filter;
  protected final List<StorageScan> scans;

  protected int nextScanIndex;
  protected StorageScan currentScan;
  private StorageScanner scanner;
  private boolean skipInvalidRecord;


  public BaseSchemaScanner(SchemaScan scan, StorageConnection conn)
      throws ValorException {
    this.conn = conn;
    this.filter = scan.getFilter() == null ? (t) -> true : scan.getFilter();
    this.scans = scan.getFragments();
    this.nextScanIndex = 0;
    if (scan.getConf() != null) {
      this.skipInvalidRecord = Boolean.valueOf(ValorConf.IGNORE_INVALID_RECORD.get(scan.getConf()));
    }
    try {
      moveToNextScanner();
    } catch (IOException e) {
      throw new ValorException("failed to get initial scanner", e);
    }
  }

  @Override
  public void close() throws IOException {
    if (this.scanner != null) {
      this.scanner.close();
    }
  }

  @Override
  public Tuple next() throws IOException, ValorException {
    Tuple t;
    while ((t = readNextTuple(currentScan.getFields(), skipInvalidRecord)) != null) {
      if (filter.test(t)) {
        return t;
      }
    }
    t =  flushRemainingData();
    return t != null && filter.test(t) ? t : null;
  }

  protected abstract Tuple flushRemainingData() throws IOException, ValorException;

  protected abstract Tuple readNextTuple(List<String> fields, boolean skipInvalidRecord)
      throws IOException, ValorException;

  protected Record readNextRecord() throws IOException, ValorException {
    do {
      Record record = scanner.next();
      if (record != null) {
        return record;
      }
    } while (moveToNextScanner());
    return null;
  }

  private boolean moveToNextScanner() throws ValorException, IOException {
    if (scanner != null) {
      scanner.close();
    }
    if (this.nextScanIndex >= scans.size()) {
      return false;
    } else {
      this.currentScan = scans.get(nextScanIndex);
      scanner = conn.getStorageScanner(currentScan);
      nextScanIndex++;
      return true;
    }
  }
}
