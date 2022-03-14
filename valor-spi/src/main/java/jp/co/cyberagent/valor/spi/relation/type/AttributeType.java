package jp.co.cyberagent.valor.spi.relation.type;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

@SuppressWarnings("rawtypes")
public abstract class AttributeType<T> {
  public static final int NULL_BYTES_SIZE = -1;

  // TODO make a method return vint size with writing vint
  public static final int NULL_BYTES_VINT_SIZE = ByteUtils.getVIntSize(NULL_BYTES_SIZE);

  public static AttributeType<?> getPrimitiveType(String type) {
    if (StringAttributeType.NAME.equals(type)) {
      return StringAttributeType.INSTANCE;
    }
    if (LongAttributeType.NAME.equals(type)) {
      return LongAttributeType.INSTANCE;
    }
    if (IntegerAttributeType.NAME.equals(type)) {
      return IntegerAttributeType.INSTANCE;
    }
    if (DoubleAttributeType.NAME.equals(type)) {
      return DoubleAttributeType.INSTANCE;
    }
    if (FloatAttributeType.NAME.equals(type)) {
      return FloatAttributeType.INSTANCE;
    }
    if (ByteArrayAttributeType.NAME.equals(type)) {
      return ByteArrayAttributeType.INSTANCE;
    }
    if (NumberAttributeType.NAME.equals(type)) {
      return NumberAttributeType.INSTANCE;
    }
    if (JsonAttributeType.NAME.equals(type)) {
      return JsonAttributeType.INSTANCE;
    }
    if (BooleanAttributeType.NAME.equals(type)) {
      return BooleanAttributeType.INSTANCE;
    }
    return null;
  }

  public static <V> AttributeType getTypeOf(V value) {
    if (value instanceof String) {
      return StringAttributeType.INSTANCE;
    }
    if (value instanceof Long) {
      return LongAttributeType.INSTANCE;
    }
    if (value instanceof Integer) {
      return IntegerAttributeType.INSTANCE;
    }
    if (value instanceof Double) {
      return DoubleAttributeType.INSTANCE;
    }
    if (value instanceof Float) {
      return FloatAttributeType.INSTANCE;
    }
    if (value instanceof Boolean) {
      return BooleanAttributeType.INSTANCE;
    }
    throw new IllegalTypeException(
        "type of " + value + ":" + value.getClass().getCanonicalName() + " is not found");
  }

  /**
   * convert a given object to a byte array
   *
   * @return length of written bytes
   */
  public int write(DataOutput out, Object o) throws SerdeException {
    if (o == null) {
      return -1;
    }
    try {
      return doWrite(out, o);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  protected abstract int doWrite(DataOutput out, Object o) throws IOException;

  public byte[] serialize(Object o) throws SerdeException {
    if (o == null) {
      return null;
    }
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos =
        new DataOutputStream(baos)) {
      write(dos, o);
      return baos.toByteArray();
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  /**
   * convert a given byte array to an object of this type
   *
   * @throws IllegalTypeException
   */
  public T read(byte[] in, int offset, int length) throws SerdeException {
    if (length < 0) {
      return null;
    }
    try {
      return doRead(in, offset, length);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  protected abstract T doRead(byte[] in, int offset, int length) throws IOException;

  public T deserialize(byte[] source) throws SerdeException {
    return deserialize(source, 0, source.length);
  }

  public T deserialize(byte[] source, int offset, int length) throws SerdeException {
    if (source == null) {
      return null;
    }
    return read(source, offset, length);
  }

  /**
   * @return the size of bytes formatted by this formatter. -1 if the size is not constant.
   */
  public abstract int getSize();

  /**
   * get String expression of this type
   */
  public abstract String getName();

  /**
   * get a Java Class represented by this type
   */
  public abstract Class<T> getRepresentedClass();

  public abstract List<AttributeType> getGenericParameterValues();

  public abstract void addGenericElementType(AttributeType attributeType);

  public String toExpression() {
    return getName();
  }
}
