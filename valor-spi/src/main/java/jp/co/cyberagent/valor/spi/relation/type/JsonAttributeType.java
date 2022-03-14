package jp.co.cyberagent.valor.spi.relation.type;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.ByteArrayInputStream;
import java.io.DataOutput;
import java.io.IOException;

public class JsonAttributeType extends PrimitiveAttributeType<Object> {
  public static final String NAME = "json";
  public static final JsonAttributeType INSTANCE = new JsonAttributeType();

  private ObjectMapper om = new ObjectMapper();

  private JsonAttributeType() {}

  @Override
  protected int doWrite(DataOutput out, Object o) throws IOException {
    JsonNode node = om.valueToTree(o);
    ObjectWriter writer = om.writer();
    byte[] b = writer.writeValueAsBytes(node);
    out.write(b);
    return b.length;
  }

  @Override
  protected Object doRead(byte[] in, int offset, int length) throws IOException {
    ObjectReader reader = om.reader();
    JsonNode node = reader.readTree(new ByteArrayInputStream(in, offset, length));

    return om.convertValue(node, Object.class);
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
  public Class<Object> getRepresentedClass() {
    return Object.class;
  }
}
