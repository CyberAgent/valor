package jp.co.cyberagent.valor.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TupleWritable implements Writable {

  private Map<String, byte[]> values = new HashMap<>();

  public TupleWritable() {
  }

  public TupleWritable(Relation tupleDef, Tuple tuple) throws IOException {
    for (String attr : tuple.getAttributeNames()) {
      AttributeType<?> type = tupleDef.getAttributeType(attr);
      Text key = new Text(attr);
      Object val = tuple.getAttribute(attr);
      if (val != null) {
        values.put(attr, type.serialize(val));
      }
    }
  }

  public Tuple convertFromWritable(Relation relation) throws IOException {
    Tuple tuple = new TupleImpl(relation);
    for (Map.Entry<String, byte[]> entry : values.entrySet()) {
      String attr = entry.getKey();
      AttributeType type = relation.getAttributeType(attr);
      byte[] value = entry.getValue();
      if (entry != null) {
        tuple.setAttribute(attr, type.deserialize(value));
      }
    }
    return tuple;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int size = values.size();
    out.writeInt(size);
    for (Map.Entry<String, byte[]> entry : values.entrySet()) {
      ByteUtils.writeByteArray(out, ByteUtils.toBytes(entry.getKey()));
      ByteUtils.writeByteArray(out, entry.getValue());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int size = in.readInt();
    values = new HashMap<>();
    for (int i = 0; i < size; i++) {
      String attr = ByteUtils.toString(ByteUtils.readByteArray(in));
      byte[] value = ByteUtils.readByteArray(in);
      values.put(attr, value);
    }
  }
}

