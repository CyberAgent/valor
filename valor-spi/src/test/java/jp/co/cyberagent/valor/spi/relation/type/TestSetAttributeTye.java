package jp.co.cyberagent.valor.spi.relation.type;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

@SuppressWarnings( {"unchecked", "rawtypes"})
public class TestSetAttributeTye extends AttributeTypeTestBase<Set> {
  private StringAttributeType strAttrType = StringAttributeType.INSTANCE;

  public TestSetAttributeTye() {
    attrType = new SetAttributeType();
  }

  @Test
  public void testToBytesFromBytes() throws SerdeException, IOException {
    attrType.addGenericElementType(StringAttributeType.INSTANCE);
    Set<String> expected = new HashSet();
    expected.add("abc");
    expected.add("12345");
    byte[] expectedBytes = toBytes(expected);
    testSerde(expected, expectedBytes);
  }

  @Test
  public void testToBytesFromBytesWithNullElement() throws SerdeException, IOException {
    attrType.addGenericElementType(StringAttributeType.INSTANCE);
    Set<String> expected = new HashSet();
    expected.add("abc");
    expected.add(null);
    expected.add("12345");
    byte[] expectedBytes = toBytes(expected);
    testSerde(expected, expectedBytes);
  }

  private byte[] toBytes(Set<String> s) throws IOException, SerdeException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    SortedSet<String> buf = new TreeSet((Comparator<String>) (o1, o2) -> {
      if (o1 == null) {
        return o2 == null ? 0 : 1;
      } else if (o2 == null) {
        return -1;
      } else {
        return o1.compareTo(o2);
      }
    });
    buf.addAll(s);
    for (String e : buf) {
      if (e == null) {
        ByteUtils.writeVInt(dos, -1);
      } else {
        ByteUtils.writeByteArray(dos, strAttrType.serialize(e));
      }
    }
    return baos.toByteArray();
  }
}
