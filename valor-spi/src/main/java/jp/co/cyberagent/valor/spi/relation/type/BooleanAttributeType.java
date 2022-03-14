package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;

public class BooleanAttributeType extends PrimitiveAttributeType<Boolean> {
  public static final String NAME = "boolean";

  public static final int SIZE = 1;

  public static final BooleanAttributeType INSTANCE = new BooleanAttributeType();

  private BooleanAttributeType() {
  }

  @Override
  protected int doWrite(DataOutput out, Object o) throws IOException {
    if (o instanceof Boolean) {
      out.writeBoolean((boolean) o);
      return SIZE;
    }
    throw new IllegalTypeException("Boolean is expected but " + o.getClass().getCanonicalName());
  }

  @Override
  protected Boolean doRead(byte[] in, int offset, int length) throws IOException {
    if (length != SIZE) {
      throw new IllegalTypeException("boolean in bytes is expected but received " + length
          + " bytes");
    }
    return in[offset] != (byte)0;
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
  public Class<Boolean> getRepresentedClass() {
    return Boolean.class;
  }
}
