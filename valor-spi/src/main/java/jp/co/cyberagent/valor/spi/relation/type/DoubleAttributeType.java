package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class DoubleAttributeType extends PrimitiveAttributeType<Double> {
  public static final String NAME = "double";

  public static final int SIZE = 8;

  public static final DoubleAttributeType INSTANCE = new DoubleAttributeType();

  private DoubleAttributeType() {
  }

  @Override
  protected int doWrite(DataOutput out, Object o) throws IllegalTypeException, IOException {
    if (o instanceof Double) {
      out.writeDouble((Double) o);
      return SIZE;
    }
    throw new IllegalTypeException("Double is expected but " + o.getClass().getCanonicalName());
  }

  @Override
  protected Double doRead(byte[] in, int offset, int length)
      throws IllegalTypeException, IOException {
    if (length != SIZE) {
      throw new IllegalTypeException("double in bytes is expected but received " + length
          + " bytes");
    }
    return ByteUtils.toDouble(in, offset);
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
  public Class<Double> getRepresentedClass() {
    return java.lang.Double.class;
  }
}
