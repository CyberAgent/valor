package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class FloatAttributeType extends PrimitiveAttributeType<Float> {
  public static final String NAME = "float";

  public static final int SIZE = 4;

  public static final FloatAttributeType INSTANCE = new FloatAttributeType();

  private FloatAttributeType() {
  }

  @Override
  protected int doWrite(DataOutput out, Object o) throws IllegalTypeException, IOException {
    if (o instanceof Float) {
      out.writeFloat((Float) o);
      return SIZE;
    }
    throw new IllegalTypeException("Float is expected but " + o.getClass().getCanonicalName());
  }

  @Override
  protected Float doRead(byte[] in, int offset, int length)
      throws IllegalTypeException, IOException {
    if (length != SIZE) {
      throw new IllegalTypeException("float in bytes is expected but received " + length
          + " bytes");
    }
    return ByteUtils.toFloat(in, offset);
  }

  @Override
  public int getSize() {
    return SIZE;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public Class<Float> getRepresentedClass() {
    return java.lang.Float.class;
  }
}
