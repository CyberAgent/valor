package jp.co.cyberagent.valor.spi.relation.type;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

@SuppressWarnings( {"unchecked", "rawtypes"})
public class TestMapAttributeType extends AttributeTypeTestBase<Map> {

  private StringAttributeType strAttrType = StringAttributeType.INSTANCE;

  public TestMapAttributeType() {
    attrType = new MapAttributeType();
  }

  @Test
  public void testToBytesFromBytes() throws SerdeException, IOException {
    attrType.addGenericElementType(StringAttributeType.INSTANCE);
    attrType.addGenericElementType(StringAttributeType.INSTANCE);
    // use linked map to check that serialized bytes are sorted
    Map<String, String> expected = new LinkedHashMap();
    expected.put("k2", "v2");
    expected.put("k1", "v1");
    byte[] expectedBytes = toBytes(expected);
    testSerde(expected, expectedBytes);
  }

  @Test
  public void testNullValue() throws SerdeException, IOException {
    attrType.addGenericElementType(StringAttributeType.INSTANCE);
    attrType.addGenericElementType(StringAttributeType.INSTANCE);
    Map<String, String> expected = new LinkedHashMap();
    expected.put("k2", null);
    expected.put("k1", "v1");
    expected.put("emp", "");
    byte[] expectedBytes = toBytes(expected);
    testSerde(expected, expectedBytes);
  }

  private byte[] toBytes(Map<String, String> m) throws IOException, SerdeException {
    SortedSet<String> keys = new TreeSet(m.keySet());
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    for (String key : keys) {
      String val = m.get(key);
      ByteUtils.writeByteArray(dos, strAttrType.serialize(key));
      if (val == null) {
        ByteUtils.writeVInt(dos, -1);
      } else {
        ByteUtils.writeByteArray(dos, strAttrType.serialize(val));
      }
    }
    return baos.toByteArray();
  }
}
