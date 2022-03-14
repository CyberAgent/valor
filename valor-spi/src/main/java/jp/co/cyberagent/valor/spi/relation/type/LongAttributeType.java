package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class LongAttributeType extends PrimitiveAttributeType<Long> {

  public static final String NAME = "long";

  public static final int SIZE = 8;

  public static final LongAttributeType INSTANCE = new LongAttributeType();

  private LongAttributeType() {
  }

  @Override
  protected int doWrite(DataOutput out, Object o) throws IllegalTypeException, IOException {
    if (o instanceof Long) {
      out.writeLong((Long) o);
      return SIZE;
    }
    throw new IllegalTypeException("long is expected but " + o.getClass().getCanonicalName());
  }

  @Override
  protected Long doRead(byte[] in, int offset, int length)
      throws IllegalTypeException, IOException {
    if (length != SIZE) {
      throw new IllegalTypeException("long in bytes is expected but received " + length + " bytes");
    }
    return ByteUtils.toLong(in, offset);
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
  public Class<Long> getRepresentedClass() {
    return Long.class;
  }
}
