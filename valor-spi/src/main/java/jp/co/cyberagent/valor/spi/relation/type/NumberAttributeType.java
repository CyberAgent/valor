package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class NumberAttributeType extends PrimitiveAttributeType<Number> {

  public static final String NAME = "number";

  public static final NumberAttributeType INSTANCE = new NumberAttributeType();

  private NumberAttributeType() {
  }

  @Override
  protected int doWrite(DataOutput out, Object o) throws IllegalTypeException, IOException {
    if (o instanceof Long) {
      out.writeLong((Long) o);
      return LongAttributeType.SIZE;
    } else if (o instanceof Float) {
      out.writeFloat((Float) o);
      return FloatAttributeType.SIZE;
    }
    throw new IllegalTypeException(
        "long or float is expected but " + o.getClass().getCanonicalName());
  }

  @Override
  protected Number doRead(byte[] in, int offset, int length)
      throws IllegalTypeException, IOException {
    if (length == LongAttributeType.SIZE) {
      return ByteUtils.toLong(in, offset, length);
    } else if (length == FloatAttributeType.SIZE) {
      return ByteUtils.toFloat(in, offset);
    }
    throw new IllegalTypeException("long or float is expected but recieved " + length + " bytes");
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
  public Class<Number> getRepresentedClass() {
    return Number.class;
  }
}
