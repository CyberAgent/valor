package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class IntegerAttributeType extends PrimitiveAttributeType<Integer> {
  public static final String NAME = "int";

  public static final int SIZE = 4;

  public static final IntegerAttributeType INSTANCE = new IntegerAttributeType();

  private IntegerAttributeType() {
  }

  @Override
  protected int doWrite(DataOutput out, Object o) throws IllegalTypeException, IOException {
    if (o instanceof Integer) {
      out.writeInt((Integer) o);
      return SIZE;
    }
    throw new IllegalTypeException("Integer is expected but " + o.getClass().getCanonicalName());
  }

  @Override
  protected Integer doRead(byte[] in, int offset, int length)
      throws IllegalTypeException, IOException {
    if (length != SIZE) {
      throw new IllegalTypeException("int in bytes is expected but received " + length + " bytes");
    }
    return ByteUtils.toInt(in, offset);
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
  public Class<Integer> getRepresentedClass() {
    return Integer.class;
  }
}
