package jp.co.cyberagent.valor.sdk.holder;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestVintSizePrefixHolder {

  static private final String ATTR = "attr";

  private Formatter formatter = AttributeValueFormatter.create(ATTR);
  private Segment serde = VintSizePrefixHolder.create(formatter);
  private Relation relation = ImmutableRelation.builder().relationId("test").addAttribute(ATTR,
      true, StringAttributeType.INSTANCE).build();

  @Test
  public void testSerializeAndDeserialize() throws Exception {
    byte[] expected = ByteUtils.add(new byte[] {0x0a}, ByteUtils.toBytes("2013-12-19"));
    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR, "2013-12-19");
    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(EqualBytes.equalBytes(expected)));
    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", serde);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(expected, 0, expected.length, deserializer);
    Tuple tuple = deserializer.pollTuple();
    assertEquals("2013-12-19", tuple.getAttribute("attr"));
  }

  @Test
  public void testSerializeAndDeserializeNull() throws Exception {
    byte[] expected = new byte[] {-1};
    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR, null);

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(EqualBytes.equalBytes(expected)));

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", serde);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(expected, 0, expected.length, deserializer);
    Tuple tuple = deserializer.pollTuple();
    assertThat(tuple.getAttribute("attr"), is(nullValue()));
  }

  @Test
  public void testSerializeAndDeserializeEmpty() throws Exception {
    byte[] expected = new byte[] {0x00};
    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR, "");

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(EqualBytes.equalBytes(expected)));

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", serde);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(expected, 0, expected.length, deserializer);
    Tuple tuple = deserializer.pollTuple();
    assertThat(tuple.getAttribute("attr"), equalTo(""));
  }

  @Test
  public void testDeserializeComposite() throws Exception {
    byte[] val = ByteUtils.toBytes("2013-12-19");
    byte[] other = ByteUtils.toBytes("other");
    int size = ByteUtils.getVIntSize(val.length);
    assertEquals(1, size);
    byte[] source = ByteUtils.add(new byte[] {(byte) val.length}, val);
    byte[] sourceWithOther = ByteUtils.add(source, other);

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", serde);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    int l = serde.cutAndSet(sourceWithOther, 0, sourceWithOther.length, deserializer);
    Tuple tuple = deserializer.pollTuple();
    assertThat(tuple.getAttribute("attr"), equalTo("2013-12-19"));
    byte[] remain = ByteUtils.copy(sourceWithOther, l, sourceWithOther.length - l);
    assertArrayEquals(remain, other);
  }

  @Test
  public void testDeserializeNull() throws Exception {
    byte[] other = ByteUtils.toBytes("other");
    byte[] source = new byte[] {(byte) -1};
    byte[] sourceWithOther = ByteUtils.add(source, other);

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", serde);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    int l = serde.cutAndSet(sourceWithOther, 0, sourceWithOther.length, deserializer);
    Tuple tuple = deserializer.pollTuple();
    assertThat(tuple.getAttribute("attr"), is(nullValue()));
    byte[] remain = ByteUtils.copy(sourceWithOther, l, sourceWithOther.length - l);
    assertArrayEquals(remain, other);
  }
}
