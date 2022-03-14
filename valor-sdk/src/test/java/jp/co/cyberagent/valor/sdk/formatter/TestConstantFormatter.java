package jp.co.cyberagent.valor.sdk.formatter;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;


import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestConstantFormatter {

  @Test
  public void testDeserialize() throws Exception {
    ConstantFormatter element = ConstantFormatter.create("-");
    TupleDeserializer deserializer = mock(TupleDeserializer.class);
    byte[] val = ByteUtils.toBytes("-x-");
    int l = element.cutAndSet(val, 0, val.length, deserializer);
    assertThat(l, equalTo(1));
  }

  @Test
  public void testUnicodeControlCharacter() throws Exception {
    ConstantFormatter element = new ConstantFormatter("\u0001");
    TupleDeserializer deserializer = mock(TupleDeserializer.class);
    byte[] val = new byte[] {0x01, 0x00};
    int l = element.cutAndSet(val, 0, val.length, deserializer);
    assertThat(l, equalTo(1));
  }

  @Test
  public void testUnicodeControlCharacterArray() throws Exception {
    ConstantFormatter element = new ConstantFormatter("\u0001\u0002\u0003\u0004");
    TupleDeserializer deserializer = mock(TupleDeserializer.class);
    byte[] val = new byte[] {0x01, 0x02, 0x03, 0x04, 0x00};
    int l = element.cutAndSet(val, 0, val.length, deserializer);
    assertThat(l, equalTo(4));
  }
}
