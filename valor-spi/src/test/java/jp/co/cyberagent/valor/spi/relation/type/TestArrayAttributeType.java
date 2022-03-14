package jp.co.cyberagent.valor.spi.relation.type;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

@SuppressWarnings( {"unchecked", "rawtypes"})
public class TestArrayAttributeType extends AttributeTypeTestBase<List> {

  private StringAttributeType strAttrType = StringAttributeType.INSTANCE;

  public TestArrayAttributeType() {
    attrType = new ArrayAttributeType();
  }

  @Test
  public void testToBytesFromBytes() throws SerdeException, IOException {
    attrType.addGenericElementType(StringAttributeType.INSTANCE);
    List<String> obj = new ArrayList<String>();
    obj.add("abc");
    obj.add("12345");
    testSerde(obj, toBytes(obj));
  }

  @Test
  public void testToBytesFromBytesWithNullElement() throws SerdeException, IOException {
    attrType.addGenericElementType(StringAttributeType.INSTANCE);
    List<String> obj = new ArrayList<String>();
    obj.add("abc");
    obj.add(null);
    obj.add("12345");
    testSerde(obj, toBytes(obj));
  }

  private byte[] toBytes(List<String> s) throws IOException, SerdeException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dos =
        new DataOutputStream(baos)) {
      for (String e : s) {
        if (e == null) {
          ByteUtils.writeVInt(dos, -1);
        } else {
          ByteUtils.writeByteArray(dos, strAttrType.serialize(e));
        }
      }
      return baos.toByteArray();
    }
  }
}
