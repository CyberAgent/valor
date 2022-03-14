package jp.co.cyberagent.valor.hbase.storage;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.FieldComparator.Operator;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.RegexpMatchSegment;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanner;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import jp.co.cyberagent.valor.spi.util.Pair;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SingleTableHBaseConnection extends HBaseConnection {

  public static final String AGGR_COPROCESSOR
      = "org.apache.hadoop.hbase.coprocessor.AggregateImplementation";

  private static final Logger LOG = LoggerFactory.getLogger(SingleTableHBaseConnection.class);

  private static final LongColumnInterpreter li = new LongColumnInterpreter();

  private AggregationClient aggrClient;

  private TableName tableName;

  public SingleTableHBaseConnection(TableName tableName, Connection conn) throws ValorException {
    super(conn);
    this.tableName = tableName;
    try (Admin admin = conn.getAdmin()) {
      HTableDescriptor td = admin.getTableDescriptor(tableName);
      if (td.getCoprocessors().contains(AGGR_COPROCESSOR)) {
        this.aggrClient = new AggregationClient(connection.getConfiguration());
      }
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      if (aggrClient != null) {
        aggrClient.close();
      }
    } finally {
      super.close();
    }
  }

  @Override
  public void insert(Collection<Record> records) throws ValorException {
    if (records.isEmpty()) {
      return;
    }

    List<Put> puts = new ArrayList<>(records.size());
    for (Record record : records) {
      Put put = new Put(record.getBytes(HBaseCell.ROWKEY));
      if (record.getBytes(HBaseCell.TIMESTAMP) == null) {
        put.addColumn(record.getBytes(HBaseCell.FAMILY), record.getBytes(HBaseCell.QUALIFIER),
                record.getBytes(HBaseCell.VALUE));
      } else {
        put.addColumn(
            record.getBytes(HBaseCell.FAMILY),
            record.getBytes(HBaseCell.QUALIFIER),
            ByteUtils.toLong(record.getBytes(HBaseCell.TIMESTAMP)),
            record.getBytes(HBaseCell.VALUE));
      }
      puts.add(put);
    }
    try (Table table = connection.getTable(tableName)) {
      table.put(puts);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  @Override
  public void delete(Collection<Record> records) throws ValorException {
    // TODO HBaseCell.TIMESTAMP is not currently supported
    if (records.isEmpty()) {
      return;
    }
    // TODO optimize deletes for same row
    List<Delete> deletes = new ArrayList<>(records.size());
    // decrement timestamp to avoid races with record insert to update tuple
    long ts = System.currentTimeMillis() - 1;
    for (Record record : records) {
      Delete delete = new Delete(record.getBytes(HBaseCell.ROWKEY));
      delete.setTimestamp(ts);
      byte[] family = record.getBytes(HBaseCell.FAMILY);
      if (family != null) {
        byte[] qualifier = record.getBytes(HBaseCell.QUALIFIER);
        if (qualifier == null) {
          delete.addFamily(family);
        } else {
          delete.addColumns(family, qualifier);
        }
      }
      deletes.add(delete);
    }
    try (Table table = connection.getTable(tableName)) {
      table.delete(deletes);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  @Override
  public Optional<Long> count(SchemaScan scan) throws ValorException {
    if (!validateSchema(scan.getSchema())) {
      LOG.info("schema {} is not countable with coprocessor (family or qualifier is not constant)",
              scan.getSchema().getSchemaId());
      return Optional.empty();
    }
    try {
      long sum = 0L;
      for (StorageScan ss : scan.getFragments()) {
        HBaseScan hbaseScan = toHBaseScan(tableName, ss);
        sum += aggrClient.rowCount(tableName, li, hbaseScan.getScan());
      }
      return Optional.of(sum);
    } catch (Throwable throwable) {
      if (aggrClient == null) {
        throw new ValorException("aggregation client is not initiated ("
            + AGGR_COPROCESSOR + " is not available in table " + tableName + ")");
      } else {
        throw new ValorException(throwable);
      }
    }
  }

  private boolean validateSchema(Schema schema) {
    if (validateLayout(schema.getLayout(HBaseCell.FAMILY))) {
      return validateLayout(schema.getLayout(HBaseCell.QUALIFIER));
    }
    return false;
  }

  private boolean validateLayout(FieldLayout layout) {
    for (Segment segment : layout.formatters()) {
      if (!(segment.getFormatter() instanceof ConstantFormatter)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public StorageScanner getStorageScanner(StorageScan scan) throws IOException, ValorException {
    HBaseScan hbaseScan = toHBaseScan(tableName, scan);
    Table table = connection.getTable(tableName);
    ResultScanner scanner = table.getScanner(hbaseScan.getScan());
    return new HBaseScanner(table, scanner);
  }

  @Override
  public List<StorageScan> split(StorageScan scan) throws ValorException {
    NavigableMap<byte[], HRegionInfo> regions = new TreeMap(ByteUtils.BYTES_COMPARATOR);
    try (Admin admin = connection.getAdmin()) {
      for (HRegionInfo regionInfo : admin.getTableRegions(tableName)) {
        regions.put(regionInfo.getStartKey(), regionInfo);
      }
    } catch (IOException e) {
      throw new ValorException(e);
    }
    HBaseScan hbaseScan = toHBaseScan(tableName, scan);
    byte[] startKey = hbaseScan.getScan().getStartRow();
    byte[] endKey = hbaseScan.getScan().getStopRow();
    List<StorageScan> splittedScans = new ArrayList<>();
    HRegionInfo regionInfo = regions.floorEntry(startKey).getValue();
    StorageScan split = scan;
    while (regionInfo != null) {
      if (regionInfo.containsRange(startKey, endKey)) {
        splittedScans.add(split);
        return splittedScans;
      } else {
        LOG.info("spliting {} at {}", scan, regionInfo.getEndKey());
        Pair<StorageScan, StorageScan> splitted = splitScan(split, regionInfo.getEndKey());
        startKey = regionInfo.getEndKey();
        splittedScans.add(splitted.getFirst());
        split = splitted.getSecond();
        Map.Entry<byte[], HRegionInfo> regionEntry = regions.higherEntry(regionInfo.getStartKey());
        regionInfo = regionEntry == null ? null : regionEntry.getValue();
      }
    }
    throw new IllegalArgumentException("failed to split " + scan);
  }

  private Pair<StorageScan, StorageScan> splitScan(StorageScan scan, byte[] splitKey) {
    FieldComparator rowComparator = scan.getFieldComparator(HBaseCell.ROWKEY);
    StorageScan head = replaceRowComparator(scan,
        rowComparator.getOperator(), rowComparator.getPrefix(),
        rowComparator.getStart(), splitKey, rowComparator.getRegexp());
    StorageScan tail = replaceRowComparator(scan,
        rowComparator.getOperator(), rowComparator.getPrefix(),
        splitKey, rowComparator.getStop(), rowComparator.getRegexp());
    return new Pair<>(head, tail);
  }

  private StorageScan replaceRowComparator(
      StorageScan orgScan, FieldComparator.Operator op,
      byte[] prefix, byte[] start, byte[] stop, byte[] regexp) {
    FieldComparator newComparator = FieldComparator.build(op, prefix, start, stop, regexp);
    List<String> fields = orgScan.getFields();
    Map<String, FieldComparator> comparators = new HashMap<>();
    for (String field : fields) {
      if (HBaseCell.ROWKEY.equals(field)) {
        comparators.put(field, newComparator);
      } else {
        FieldComparator c = orgScan.getFieldComparator(field);
        if (c != null) {
          comparators.put(field, c);
        }
      }
    }
    return StorageScan.build(fields, comparators);
  }

  @VisibleForTesting
  public static HBaseScan toHBaseScan(TableName tableName, StorageScan ss) throws ValorException {
    // TODO HBaseCell.TIMESTAMP is not currently supported

    Scan scan = new Scan();
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    FieldComparator comparator = ss.getFieldComparator(HBaseCell.ROWKEY);
    if (comparator != null) {
      LOG.debug("FROM {} TO {} ", ByteUtils.toStringBinary(comparator.getStart()),
          ByteUtils.toStringBinary(comparator.getStop()));
      if (comparator.getStart() != null) {
        scan.setStartRow(comparator.getStart());
      }
      if (comparator.getStop() != null) {
        scan.setStopRow(comparator.getStop());
      }
      if (comparator.getRegexp() != null) {
        byte[] regexp = comparator.getRegexp();
        if (regexp.length > 0 && !ByteUtils.equals(regexp, RegexpMatchSegment.WILDCARD)) {
          filterList.addFilter(new RowFilter(CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(ByteUtils.toString(regexp))));
        }
      }
    } else {
      LOG.warn("row comparator is not set in " + ss);
    }

    comparator = ss.getFieldComparator(HBaseCell.FAMILY);
    byte[] family = null;
    if (comparator != null) {
      if (Operator.EQUAL.equals(comparator.getOperator())) {
        family = comparator.getStart();
      } else {
        byte[] filterValue = comparator.getPrefix();
        if (filterValue != null && filterValue.length > 0) {
          filterList.addFilter(new FamilyFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(filterValue)));
        }
        filterValue = comparator.getRegexp();
        if (filterValue != null && filterValue.length > 0 && !ByteUtils.equals(filterValue,
            RegexpMatchSegment.WILDCARD)) {
          filterList.addFilter(new FamilyFilter(CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(ByteUtils.toString(filterValue))));
        }
      }
    }

    comparator = ss.getFieldComparator(HBaseCell.QUALIFIER);
    byte[] qualifier = null;
    if (comparator != null) {
      if (Operator.EQUAL.equals(comparator.getOperator())) {
        qualifier = comparator.getPrefix();
      } else {
        byte[] filterValue = comparator.getPrefix();
        if (filterValue != null && filterValue.length > 0) {
          filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(filterValue)));
        }
        filterValue = comparator.getRegexp();
        if (filterValue != null && filterValue.length > 0 && !ByteUtils.equals(filterValue,
            RegexpMatchSegment.WILDCARD)) {
          filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
              new RegexStringComparator(ByteUtils.toString(filterValue))));
        }
      }
    }

    comparator = ss.getFieldComparator(HBaseCell.VALUE);
    if (comparator != null) {
      byte[] filterValue = comparator.getPrefix();
      if (filterValue != null && filterValue.length > 0) {
        if (Operator.EQUAL.equals(comparator.getOperator())) {
          filterList.addFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryComparator(filterValue)));
        } else {
          filterList.addFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL,
              new BinaryPrefixComparator(filterValue)));
        }
      }
      filterValue = comparator.getRegexp();
      if (filterValue != null && filterValue.length > 0 && !ByteUtils.equals(filterValue,
          RegexpMatchSegment.WILDCARD)) {
        filterList.addFilter(new ValueFilter(CompareFilter.CompareOp.EQUAL,
            new RegexStringComparator(ByteUtils.toString(filterValue))));
      }
    }

    if (family != null) {
      if (qualifier != null) {
        scan.addColumn(family, qualifier);
      } else {
        scan.addFamily(family);
      }
    } else if (qualifier != null) {
      filterList.addFilter(new QualifierFilter(CompareFilter.CompareOp.EQUAL,
          new BinaryComparator(qualifier)));
    }
    if (!ss.getFields().contains(HBaseCell.VALUE)) {
      filterList.addFilter(new KeyOnlyFilter());
    }

    if (filterList.getFilters().size() > 0) {
      scan.setFilter(filterList);
    }
    LOG.info("HBase scan {} is generated from storage scan fragment {}", scan, ss);
    return new HBaseScan(tableName.getName(), scan);
  }
}
