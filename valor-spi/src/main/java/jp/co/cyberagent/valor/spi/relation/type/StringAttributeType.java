package jp.co.cyberagent.valor.spi.relation.type;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;

public class StringAttributeType extends PrimitiveAttributeType<String> {

  public static final String NAME = "string";

  public static final String CHARSET_NAME = StandardCharsets.UTF_8.name();

  public static final StringAttributeType INSTANCE = new StringAttributeType();

  private StringAttributeType() {
  }

  @Override
  protected int doWrite(DataOutput out, Object o) throws IllegalTypeException, IOException {
    if (o instanceof String) {
      byte[] b = ((String) o).getBytes(CHARSET_NAME);
      out.write(b);
      return b.length;
    }
    throw new IllegalTypeException("String is expected but " + o.getClass().getCanonicalName());
  }

  @Override
  protected String doRead(byte[] in, int offset, int length) throws IOException {
    return new String(in, offset, length, CHARSET_NAME);
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
  public Class<String> getRepresentedClass() {
    return String.class;
  }
}
