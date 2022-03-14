package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

@SuppressWarnings({"rawtypes", "unchecked"})
public class ArrayAttributeType extends AttributeType<List> {

  public static final String NAME = "array";

  public static ArrayAttributeType create(AttributeType elementType) {
    ArrayAttributeType type = new ArrayAttributeType();
    type.addGenericElementType(elementType);
    return type;
  }

  private AttributeType elementType;

  @Override
  protected int doWrite(DataOutput out, Object o) throws IOException {
    if (!(o instanceof List)) {
      throw new IllegalTypeException("List is expected but " + o.getClass().getCanonicalName());
    }
    int size = 0;
    for (Object e : (List) o) {
      if (e == null) {
        ByteUtils.writeVInt(out, -1);
        size += NULL_BYTES_VINT_SIZE;
      } else {
        byte[] elmBytes = elementType.serialize(e);
        ByteUtils.writeVInt(out, elmBytes.length);
        out.write(elmBytes);
        size = size + ByteUtils.getVIntSize(elmBytes.length) + elmBytes.length;
      }
    }
    return size;
  }

  @Override
  protected List doRead(byte[] in, int offset, int length) throws IOException {
    List val = new ArrayList();
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
  public Class<List> getRepresentedClass() {
    return List.class;
  }

  @Override
  public void addGenericElementType(AttributeType elementType) {
    if (this.elementType != null) {
      throw new IllegalStateException(this.getClass().getCanonicalName() + " can have only 1 "
          + "generic type parameter, previously set to " + this.elementType);
    }
    this.elementType = elementType;
  }

  @Override
  public List<AttributeType> getGenericParameterValues() {
    return Arrays.asList(this.elementType);
  }

  public AttributeType getElementType() {
    return elementType;
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
    ArrayAttributeType that = (ArrayAttributeType) o;
    return Objects.equals(elementType, that.elementType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(elementType);
  }
}
