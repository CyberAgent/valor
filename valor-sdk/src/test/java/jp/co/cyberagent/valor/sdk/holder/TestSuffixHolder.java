package jp.co.cyberagent.valor.sdk.holder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import jp.co.cyberagent.valor.sdk.EqualBytes;
import jp.co.cyberagent.valor.sdk.ReflectionUtil;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class TestSuffixHolder {

  private static final String ATTR = "attr";
  private static Formatter formatter = AttributeValueFormatter.create(ATTR);
  private static Relation relation = ImmutableRelation.builder().relationId("test").addAttribute(ATTR,
      true, StringAttributeType.INSTANCE).build();

  @ParameterizedTest
  @CsvSource({
      "-, prefix",
      "-, ''",
      "'\u0001', prefix",
      "'\u0001', 接頭辞",
      "'\u0001\u0002', 'abc\u0001def'"
  })
  public void test(String suffix, String attrValue) throws Exception {
    doTest(suffix, attrValue, true);
    doTest(suffix, attrValue, false);
  }

  public void doTest(String suffix, String attrValue, boolean fromHead) throws Exception {
    Holder holder = SuffixHolder.create(suffix, fromHead, formatter);
    if (!fromHead) {
      attrValue = "head" + suffix + attrValue;
    }
    final int expectedLength = ByteUtils.toBytes(attrValue).length + ByteUtils.toBytes(suffix).length;
    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR, attrValue);

    TupleSerializer serializer = mock(TupleSerializer.class);
    holder.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(EqualBytes.equalBytes(ByteUtils.toBytes(attrValue))),
        argThat(EqualBytes.equalBytes(ByteUtils.toBytes(suffix))));

    byte[] serialized = ByteUtils.add(ByteUtils.toBytes(attrValue), ByteUtils.toBytes(suffix));
    if (fromHead) {
      serialized = ByteUtils.add(serialized, ByteUtils.toBytes(attrValue), ByteUtils.toBytes(suffix));
    }
    TupleDeserializer deserializer = new OneToOneDeserializer(relation);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    int l = holder.cutAndSet(serialized, 0, serialized.length, deserializer);
    t = deserializer.pollTuple();
    assertEquals(attrValue, t.getAttribute(ATTR));
    assertThat(l, equalTo(expectedLength));

    if (fromHead) {
      ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
      l = holder.cutAndSet(serialized, l, serialized.length, deserializer);
      t = deserializer.pollTuple();
      assertEquals(attrValue, t.getAttribute(ATTR));
      assertThat(l, equalTo(expectedLength));
    }
  }

}
