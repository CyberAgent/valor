package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;

public class ByteArrayAttributeType extends PrimitiveAttributeType<Bytes> {
  public static final String NAME = "bytes";

  public static final ByteArrayAttributeType INSTANCE = new ByteArrayAttributeType();

  private ByteArrayAttributeType() {
  }

  @Override
  protected int doWrite(DataOutput out, Object o) throws IllegalTypeException, IOException {
    if (o instanceof Bytes) {
      Bytes b = (Bytes) o;
      out.write(b.get());
      return b.getLength();
    }
    throw new IllegalTypeException("Bytes is expected but " + o.getClass().getCanonicalName());
  }

  @Override
  protected Bytes doRead(byte[] in, int offset, int length) throws IOException {
    byte[] out = new byte[length];
    System.arraycopy(in, offset, out, 0, length);
    return new Bytes(out);
  }

  @Override
  public int getSize() {
    return -1;
  }

  @Override
  public String getName() {
    return NAME;
  }

  public Class<Bytes> getRepresentedClass() {
    return Bytes.class;
  }
}
