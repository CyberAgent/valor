package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

@SuppressWarnings({"rawtypes", "unchecked"})
public class SetAttributeType extends AttributeType<Set> {

  public static final String NAME = "set";
  private AttributeType elementType;

  public SetAttributeType() {
  }

  public SetAttributeType(AttributeType elementType) {
    this.addGenericElementType(elementType);
  }

  @Override
  protected int doWrite(DataOutput out, Object o) throws IOException {
    if (!(o instanceof Set)) {
      throw new IllegalTypeException("Set is expected but " + o.getClass().getCanonicalName());
    }

    SortedSet<byte[]> sortBuf = new TreeSet(ByteUtils.BYTES_COMPARATOR);
    boolean hasNull = false;
    for (Object e : (Set) o) {
      if (e == null) {
        hasNull = true;
      } else {
        byte[] elmBytes = elementType.serialize(e);
        sortBuf.add(elmBytes);
      }
    }
    int size = 0;
    for (byte[] elmBytes : sortBuf) {
      ByteUtils.writeVInt(out, elmBytes.length);
      out.write(elmBytes);
      size = size + ByteUtils.getVIntSize(elmBytes.length) + elmBytes.length;
    }
    if (hasNull) {
      ByteUtils.writeVInt(out, -1);
      size += NULL_BYTES_VINT_SIZE;
    }
    return size;
  }

  @Override
  protected Set doRead(byte[] in, int offset, int length) throws IOException {
    Set val = new HashSet();
    int position = offset;
    while (position < offset + length) {
      int size = ByteUtils.readVInt(in, position);
      position += ByteUtils.getVIntSize(size);
      if (size < 0) {
        val.add(null);
      } else {
        val.add(this.elementType.read(in, position, size));
        position += size;
      }
    }
    return val;
  }

  @Override
  public int getSize() {
    return -1;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Class<Set> getRepresentedClass() {
    return Set.class;
  }

  @Override
  public void addGenericElementType(AttributeType elementType) {
    if (this.elementType != null) {
      throw new IllegalStateException(this.getClass()
          .getCanonicalName() + " can have only 1 generic type parameter, previously set to "
          + this.elementType);
    }
    this.elementType = elementType;
  }

  @Override
  public List<AttributeType> getGenericParameterValues() {
    return Arrays.asList(this.elementType);
  }

  @Override
  public String toExpression() {
    return String.format("%s<%s>", NAME, elementType.toExpression());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SetAttributeType that = (SetAttributeType) o;
    return Objects.equals(elementType, that.elementType);
  }

  @Override
  public int hashCode() {

    return Objects.hash(elementType);
  }
}
