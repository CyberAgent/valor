package jp.co.cyberagent.valor.hive;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hive.serde2.StructObject;

/**
 *
 */
public class TupleStruct implements StructObject {

  private final Object[] values;

  public TupleStruct(int size) {
    this.values = new Object[size];
  }

  public void set(int index, Object value) {
    this.values[index] = value;
  }

  @Override
  public Object getField(int i) {
    return values[i];
  }

  @Override
  public List<Object> getFieldsAsList() {
    return Arrays.asList(values);
  }

  @Override
  public String toString() {
    return Arrays.toString(values);
  }
}
