package jp.co.cyberagent.valor.spi.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;

public class StorageScanImpl implements StorageScan {

  private final List<String> fields;

  private Map<String, FieldComparator> comparators = new HashMap<>();

  public StorageScanImpl(List<String> fields) {
    this.fields = fields;
  }

  public StorageScanImpl(List<String> fields, Map<String, FieldComparator> comparators) {
    this.fields = fields;
    this.comparators = comparators;
  }

  public StorageScanImpl(StorageScan org) {
    this.fields = new ArrayList<>(org.getFields());
    this.comparators = new HashMap<>();
    for (String field : fields) {
      this.comparators.put(field, FieldComparator.build(org.getFieldComparator(field)));
    }
  }

  @Override
  public List<String> getFields() {
    return fields;
  }

  @Override
  public FieldComparator getFieldComparator(String fieldName) {
    return comparators.get(fieldName);
  }

  @Override
  public void update(String fieldName, List<FilterSegment> filterSegments) {
    if (comparators.containsKey(fieldName)) {
      throw new IllegalStateException("duplicated field comparator for  " + fieldName);
    }
    FieldComparator comparator = new FieldComparator();
    for (FilterSegment fragment : filterSegments) {
      comparator.append(fragment);
    }
    comparators.put(fieldName, comparator);
  }

  @Override
  public void mergeByOr(StorageScan other) {
    List<String> otherFields = other.getFields();
    List<String> fields = this.fields.size() > otherFields.size() ? this.fields : otherFields;
    for (String field : fields) {
      FieldComparator thisComparator = comparators.get(field);
      FieldComparator otherComparator = other.getFieldComparator(field);
      FieldComparator mergedComparator = thisComparator == null || otherComparator == null
          ? null : thisComparator.mergeByOr(otherComparator);
      comparators.put(field, mergedComparator);
    }
  }

  @Override
  public String toString() {
    StringBuilder compExp = new StringBuilder();
    compExp.append("[");
    for (String field : fields) {
      compExp.append(field).append(" ").append(comparators.get(field)).append(", ");
    }
    compExp.append("]");
    return compExp.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StorageScanImpl)) {
      return false;
    }
    StorageScanImpl that = (StorageScanImpl) o;
    return Objects.equals(fields, that.fields) && Objects.equals(comparators, that.comparators);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields, comparators);
  }
}
