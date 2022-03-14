package jp.co.cyberagent.valor.spi.storage;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;

public interface StorageScan {

  byte[] START_BYTES = new byte[0];
  byte[] END_BYTES = null;

  Comparator<byte[]> UNSINGED_BYTES_COMPARATOR = (o1, o2) -> {
    if (o1 == END_BYTES) {
      return o2 == END_BYTES ? 0 : 1;
    }
    if (o2 == END_BYTES) {
      return -1;
    }
    int size = o1.length < o2.length ? o1.length : o2.length;
    for (int i = 0; i < size; i++) {
      int v1 = (o1[i] & 0xff);
      int v2 = (o2[i] & 0xff);
      if (v1 != v2) {
        return v1 - v2;
      }
    }
    return o1.length - o2.length;
  };
  Function<FieldComparator, byte[]> EXTRACT_START = o -> o == null ? START_BYTES : o.getStart();
  Function<FieldComparator, byte[]> EXTRACT_STOP = o -> {
    if (o == null) {
      return StorageScan.END_BYTES;
    }
    if (FieldComparator.Operator.EQUAL.equals(o.getOperator())) {
      return o.getStart();
    } else {
      return o.getStop();
    }
  };

  static StorageScan build(List<String> fields) {
    return new StorageScanImpl(fields);
  }

  static StorageScan build(List<String> fields, Map<String, FieldComparator> comparators) {
    return new StorageScanImpl(fields, comparators);
  }

  static StorageScan build(StorageScan scan) {
    return new StorageScanImpl(scan);
  }

  void mergeByOr(StorageScan fragment);

  List<String> getFields();

  FieldComparator getFieldComparator(String fieldName);

  default byte[] getStart(String fieldName) {
    FieldComparator comparator = getFieldComparator(fieldName);
    if (comparator == null) {
      return START_BYTES;
    }
    return comparator.getStart();
  }

  default byte[] getStop(String fieldName) {
    FieldComparator comparator = getFieldComparator(fieldName);
    if (comparator == null) {
      return END_BYTES;
    }
    return comparator.getStop();
  }

  default byte[] getRegexp(String fieldName) {
    FieldComparator comparator = getFieldComparator(fieldName);
    if (comparator == null) {
      return null;
    }
    return comparator.getRegexp();
  }

  void update(String fieldName, List<FilterSegment> filterSegments);

  class StorageScanFragmentComparator implements Comparator<StorageScan> {

    private final List<String> fields;
    private final Function<FieldComparator, byte[]> leftFunction;
    private final Function<FieldComparator, byte[]> rightFunction;

    public StorageScanFragmentComparator(List<String> fields) {
      this(fields, EXTRACT_START, EXTRACT_START);
    }

    public StorageScanFragmentComparator(List<String> fields,
                                         Function<FieldComparator, byte[]> leftFunction,
                                         Function<FieldComparator, byte[]> rightFunction) {
      this.fields = fields;
      this.leftFunction = leftFunction;
      this.rightFunction = rightFunction;
    }

    @Override
    public int compare(StorageScan left, StorageScan right) {
      for (String field : fields) {
        FieldComparator leftFieldComparator = left.getFieldComparator(field);
        FieldComparator rightFieldComparator = right.getFieldComparator(field);
        if (leftFieldComparator == null && rightFieldComparator == null) {
          return 0;
        }
        byte[] leftValue = leftFunction.apply(leftFieldComparator);
        byte[] rightValue = rightFunction.apply(rightFieldComparator);
        int c = UNSINGED_BYTES_COMPARATOR.compare(leftValue, rightValue);
        if (c != 0) {
          return c;
        }
      }
      return 0;
    }
  }
}
