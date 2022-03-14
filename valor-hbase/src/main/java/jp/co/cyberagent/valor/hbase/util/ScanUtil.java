package jp.co.cyberagent.valor.hbase.util;

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.TreeSet;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

public class ScanUtil {

  public static Scan merge(Scan scan1, Scan scan2) {
    Scan merged = new Scan();
    // set start row
    byte[] thisStartRow = scan1.getStartRow();
    byte[] otherStartRow = scan2.getStartRow();
    if (ByteUtils.compareTo(thisStartRow, otherStartRow) < 0) {
      merged.setStartRow(thisStartRow);
    } else {
      merged.setStartRow(otherStartRow);
    }

    // set stop row
    byte[] thisStopRow = scan1.getStopRow();
    byte[] otherStopRow = scan2.getStopRow();
    // CHECKSTYLE:OFF
    if (ByteUtils.equals(thisStopRow, HConstants.EMPTY_END_ROW) || ByteUtils
        .equals(otherStopRow, HConstants.EMPTY_END_ROW)) {
    } else if (ByteUtils.compareTo(thisStopRow, otherStopRow) < 0) {
      merged.setStopRow(otherStopRow);
    } else {
      merged.setStopRow(thisStopRow);
    }
    // CHECKSTYLE:ON

    // set family map
    Map<byte[], NavigableSet<byte[]>> thisFamilyMap = scan1.getFamilyMap();
    Map<byte[], NavigableSet<byte[]>> otherFamilyMap = scan2.getFamilyMap();
    if (!thisFamilyMap.isEmpty() || !otherFamilyMap.isEmpty()) {
      SortedSet<byte[]> families =
          new TreeSet<byte[]>(ByteUtils.BYTES_COMPARATOR);
      families.addAll(thisFamilyMap.keySet());
      families.addAll(otherFamilyMap.keySet());
      for (byte[] family : families) {
        NavigableSet<byte[]> thisQualifiers = thisFamilyMap.get(family);
        NavigableSet<byte[]> otherQualifiers = otherFamilyMap.get(family);
        if (thisQualifiers != null && otherQualifiers != null) {
          SortedSet<byte[]> qualifiers =
              new TreeSet<byte[]>(ByteUtils.BYTES_COMPARATOR);
          qualifiers.addAll(thisQualifiers);
          qualifiers.addAll(otherQualifiers);
          if (qualifiers.isEmpty()) {
            merged.addFamily(family);
          } else {
            for (byte[] qualifier : qualifiers) {
              merged.addColumn(family, qualifier);
            }
          }
        }
      }
    }

    // set filters
    Filter thisFilter = scan1.getFilter();
    Filter otherFilter = scan2.getFilter();
    if (thisFilter != null && otherFilter != null) {
      FilterList mergedFilter = new FilterList(FilterList.Operator.MUST_PASS_ONE);
      mergedFilter = ScanUtil.addToFilterList(mergedFilter, thisFilter);
      mergedFilter = ScanUtil.addToFilterList(mergedFilter, otherFilter);
      if (!mergedFilter.getFilters().isEmpty()) {
        merged.setFilter(mergedFilter);
      }
    }
    return merged;
  }

  public static FilterList addToFilterList(FilterList list, Filter filter) {
    if (filter instanceof FilterList) {
      if (((FilterList) filter).getFilters().size() > 0) {
        list.addFilter(filter);
      }
    } else if (filter != null) {
      list.addFilter(filter);
    }
    return list;
  }

  public static String filterToString(Filter f, int indent) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < indent; i++) {
      buf.append(" ");
    }
    if (f instanceof FilterList) {
      buf.append(((FilterList) f).getOperator().name());
      buf.append(System.getProperty("line.separator"));
      for (Filter cf : ((FilterList) f).getFilters()) {
        buf.append(filterToString(cf, indent + 1));
        buf.append(System.getProperty("line.separator"));
      }
    } else {
      if (f instanceof CompareFilter) {
        buf.append(f.getClass().getCanonicalName());
        buf.append(" ");
        buf.append(((CompareFilter) f).getOperator().name());
        buf.append(" ");
        buf.append(((CompareFilter) f).getComparator().getClass().getCanonicalName());
        buf.append(" ");
        buf.append(ByteUtils
            .toStringBinary(((CompareFilter) f).getComparator().getValue()));
      } else if (f != null) {
        buf.append(f.toString());
      }
    }
    return buf.toString();
  }

  /**
   * split a schema scan for each region
   *
   * @throws IOException
   */
  public static List<Scan> split(Scan scan, RegionLocator regionLocator) throws IOException {
    List<Scan> splits = new ArrayList<Scan>();
    byte[][] startKeys = regionLocator.getStartKeys();
    byte[] start = scan.getStartRow();
    byte[] stop = scan.getStopRow();
    for (int i = 0; i < startKeys.length; i++) {
      if (i == startKeys.length - 1) {
        // last region
        Scan split = copyScanWithNewRange(scan, start, stop);
        splits.add(split);
      } else if (isInRange(startKeys[i], startKeys[i + 1], start)) {
        if (!ByteUtils.equals(stop, HConstants.EMPTY_END_ROW) && isInRange(
            startKeys[i], startKeys[i + 1], stop)) {
          // contained in current region
          Scan split = copyScanWithNewRange(scan, start, stop);
          splits.add(split);
          break;
        } else {
          // interregion
          Scan split = copyScanWithNewRange(scan, start, startKeys[i + 1]);
          splits.add(split);
          start = startKeys[i + 1];
        }
      }
    }
    return splits;
  }

  private static boolean isInRange(byte[] begin, byte[] end, byte[] checked) {
    return ByteUtils
        .compareTo(begin, checked) <= 0 && ByteUtils
        .compareTo(checked, end) < 0;
  }

  private static Scan copyScanWithNewRange(Scan org, byte[] start, byte[] end) throws IOException {
    Scan newScan = new Scan(org);
    newScan.setStartRow(start);
    newScan.setStopRow(end);
    return newScan;
  }

  public static boolean isOverlap(Scan antecessor, Scan follower) {
    return ByteUtils.compareTo(follower.getStartRow(), antecessor.getStopRow()) < 0 || ByteUtils
        .equals(EMPTY_BYTE_ARRAY, antecessor.getStopRow());
  }
}
