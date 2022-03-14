package jp.co.cyberagent.valor.hbase.util;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;


import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestScanUtil {
  @Mock
  private RegionLocator regionLocator;

  @BeforeEach
  public void initMock() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testMergeQualifier() {
    Scan s1 = new Scan();
    byte[] family = ByteUtils.toBytes("f");
    byte[] qual1 = ByteUtils.toBytes("q1");
    s1.addColumn(family, qual1);
    Scan s2 = new Scan();
    byte[] qual2 = ByteUtils.toBytes("q2");
    s2.addColumn(family, qual2);
    Scan mergedScan = ScanUtil.merge(s1, s2);
    NavigableSet<byte[]> quals = mergedScan.getFamilyMap().get(family);
    assertEquals(2, quals.size());
    assertTrue(quals.contains(qual1));
    assertTrue(quals.contains(qual2));
  }

  @Test
  public void testMergeQualWithStartEndRow() {
    Scan s1 = new Scan();
    s1.setStartRow(ByteUtils.toBytes("a"));
    s1.setStopRow(ByteUtils.toBytes("b"));
    byte[] family = ByteUtils.toBytes("f");
    byte[] qual1 = ByteUtils.toBytes("q1");
    s1.addColumn(family, qual1);

    Scan s2 = new Scan();
    s2.setStartRow(ByteUtils.toBytes("a"));
    s2.setStopRow(ByteUtils.toBytes("b"));
    byte[] qual2 = ByteUtils.toBytes("q2");
    s2.addColumn(family, qual2);

    Scan merged = ScanUtil.merge(s1, s2);
    NavigableSet<byte[]> quals = merged.getFamilyMap().get(family);
    assertEquals(2, quals.size());
    assertTrue(quals.contains(qual1));
    assertTrue(quals.contains(qual2));
  }

  @Test
  public void testNotMerge() {
    Scan s1 = new Scan();
    s1.setStartRow(ByteUtils.toBytes("a"));
    s1.setStopRow(ByteUtils.toBytes("b"));
    byte[] family = ByteUtils.toBytes("f");
    byte[] qual1 = ByteUtils.toBytes("q1");
    s1.addColumn(family, qual1);

    Scan s2 = new Scan();
    s2.setStartRow(ByteUtils.toBytes("c"));
    s2.setStopRow(ByteUtils.toBytes("d"));
    byte[] qual2 = ByteUtils.toBytes("q2");
    s2.addColumn(family, qual2);

    assertFalse(ScanUtil.isOverlap(s1, s2));
  }

  @Test
  public void testSplit() throws IOException {
    when(regionLocator.getStartKeys()).thenReturn(new byte[][] {HConstants.EMPTY_START_ROW,
        ByteUtils.toBytes("b"), ByteUtils.toBytes("c")});
    Scan scan = new Scan();
    List<Scan> splits = ScanUtil.split(scan, regionLocator);
    assertEquals(3, splits.size());
    Iterator<Scan> itr = splits.iterator();
    Scan s = itr.next();
    assertArrayEquals(HConstants.EMPTY_START_ROW, s.getStartRow());
    assertArrayEquals(ByteUtils.toBytes("b"), s.getStopRow());
    s = itr.next();
    assertArrayEquals(ByteUtils.toBytes("b"), s.getStartRow());
    assertArrayEquals(ByteUtils.toBytes("c"), s.getStopRow());
    s = itr.next();
    assertFalse(itr.hasNext());
    assertArrayEquals(ByteUtils.toBytes("c"), s.getStartRow());
    assertArrayEquals(HConstants.EMPTY_END_ROW, s.getStopRow());
  }

  @Test
  public void testSplitToOnScan() throws IOException {
    when(regionLocator.getStartKeys()).thenReturn(new byte[][] {HConstants.EMPTY_START_ROW,
        ByteUtils.toBytes("b"), ByteUtils.toBytes("c")});
    Scan scan = new Scan(ByteUtils.toBytes("b1"),
        ByteUtils.toBytes("b2"));
    List<Scan> splits = ScanUtil.split(scan, regionLocator);
    assertEquals(1, splits.size());
    Iterator<Scan> itr = splits.iterator();
    Scan s = itr.next();
    assertFalse(itr.hasNext());
    assertArrayEquals(ByteUtils.toBytes("b1"), s.getStartRow());
    assertArrayEquals(ByteUtils.toBytes("b2"), s.getStopRow());
  }

  @Test
  public void testSplitToOnlyLastRegion() throws IOException {
    when(regionLocator.getStartKeys()).thenReturn(new byte[][] {HConstants.EMPTY_START_ROW,
        ByteUtils.toBytes("b"), ByteUtils.toBytes("c")});
    Scan scan = new Scan(ByteUtils.toBytes("c1"),
        ByteUtils.toBytes("c2"));
    List<Scan> splits = ScanUtil.split(scan, regionLocator);
    assertEquals(1, splits.size());
    Iterator<Scan> itr = splits.iterator();
    Scan s = itr.next();
    assertFalse(itr.hasNext());
    assertArrayEquals(ByteUtils.toBytes("c1"), s.getStartRow());
    assertArrayEquals(ByteUtils.toBytes("c2"), s.getStopRow());
  }
}
