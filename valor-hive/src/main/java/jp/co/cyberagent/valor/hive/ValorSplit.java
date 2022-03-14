package jp.co.cyberagent.valor.hive;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanImpl;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;

public class ValorSplit extends FileSplit {

  private static final byte NULL_MARKER = 0x00;
  private static final byte EXIST_MARKER = 0x01;

  private String relationId;

  private String schemaId;

  private StorageScan scanFragment;

  public ValorSplit() {
    super((Path) null, 0, 0, (String[]) null);
  }

  public ValorSplit(Path dummyPath, String relationId, String schemaId, StorageScan scanFragment) {
    super(dummyPath, 0, 0, (String[]) null);
    this.relationId = relationId;
    this.schemaId = schemaId;
    this.scanFragment = scanFragment;
  }

  public String getRelationId() {
    return relationId;
  }

  public String getSchemaId() {
    return schemaId;
  }

  public StorageScan getScanFragment() {
    return scanFragment;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    ByteUtils.writeByteArray(out, ByteUtils.toBytes(relationId));
    ByteUtils.writeByteArray(out, ByteUtils.toBytes(schemaId));
    List<String> fields = scanFragment.getFields();
    out.writeInt(fields.size());
    for (String field : fields) {
      ByteUtils.writeByteArray(out, ByteUtils.toBytes(field));
      FieldComparator comparator = scanFragment.getFieldComparator(field);
      if (comparator == null) {
        out.writeByte(NULL_MARKER);
      } else {
        out.writeByte(EXIST_MARKER);
        writeFieldComparator(out, scanFragment.getFieldComparator(field));
      }
    }
  }

  private void writeFieldComparator(DataOutput out, FieldComparator fieldComparator)
      throws IOException {
    ByteUtils.writeByteArray(out, ByteUtils.toBytes(fieldComparator.getOperator().name()));
    writeBytes(out, fieldComparator.getPrefix());
    writeBytes(out, fieldComparator.getStart());
    writeBytes(out, fieldComparator.getStop());
    writeBytes(out, fieldComparator.getRegexp());
  }

  private void writeBytes(DataOutput out, byte[] val) throws IOException {
    if (val == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(val.length);
      out.write(val);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.relationId = ByteUtils.toString(ByteUtils.readByteArray(in));
    this.schemaId = ByteUtils.toString(ByteUtils.readByteArray(in));
    int fieldSize = in.readInt();
    List<String> fields = new ArrayList<>(fieldSize);
    Map<String, FieldComparator> comparators = new HashMap<>();
    for (int i = 0; i < fieldSize; i++) {
      String field = ByteUtils.toString(ByteUtils.readByteArray(in));
      fields.add(field);
      byte marker = in.readByte();
      if (marker == EXIST_MARKER) {
        comparators.put(field, readFieldComparator(in));
      }
    }
    this.scanFragment = new StorageScanImpl(fields, comparators);
  }

  private FieldComparator readFieldComparator(DataInput in) throws IOException {
    FieldComparator.Operator operator =
        FieldComparator.Operator.valueOf(ByteUtils.toString(ByteUtils.readByteArray(in)));
    byte[] prefix = readBytes(in);
    byte[] start = readBytes(in);
    byte[] stop = readBytes(in);
    byte[] regexp = readBytes(in);
    return FieldComparator.build(operator, prefix, start, stop, regexp);
  }

  private byte[] readBytes(DataInput in) throws IOException {
    int size = in.readInt();
    if (size < 0) {
      return null;
    }
    byte[] v = new byte[size];
    in.readFully(v);
    return v;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append(relationId).append(".").append(schemaId);
    buf.append("(").append(scanFragment.toString()).append(")");
    return buf.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ValorSplit)) {
      return false;
    }
    ValorSplit that = (ValorSplit) o;
    return Objects.equals(relationId, that.relationId)
        && Objects.equals(schemaId, that.schemaId)
        && Objects.equals(scanFragment, that.scanFragment);
  }

  @Override
  public int hashCode() {
    return Objects.hash(relationId, schemaId, scanFragment);
  }

}








