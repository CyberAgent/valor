package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
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

public class TestString2NumberFormatter {

  private static final String ATTR_NAME = "attr";

  private Formatter serde = new String2NumberFormatter(ATTR_NAME);
  private Relation relation =
      ImmutableRelation.builder().relationId("test").addAttribute(ATTR_NAME, true,
          StringAttributeType.INSTANCE).build();

  @Test
  public void testSerialize() throws Exception {
    long expectedLong = 1l;
    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR_NAME, Long.toString(expectedLong));
    byte[] expected = ByteUtils.toBytes(expectedLong);

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    float expectedFloat = 0.1f;
    expected = ByteUtils.toBytes(expectedFloat);
    t.setAttribute(ATTR_NAME, Float.toString(expectedFloat));
    serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }

  @Test
  public void testDeserialize() throws Exception {
    Long expectedLong = 100l;
    TupleDeserializer f = new OneToOneDeserializer(relation);
    setField(f, "tuple", new TupleImpl(relation));
    byte[] bytes = ByteUtils.toBytes(expectedLong);
    serde.cutAndSet(bytes, 0, bytes.length, f);
    Tuple t = f.pollTuple();
    assertEquals(expectedLong.toString(), t.getAttribute("attr"));

    Float expectedFloat = 0.01f;
    bytes = ByteUtils.toBytes(expectedFloat);
    setField(f, "tuple", new TupleImpl(relation));
    serde.cutAndSet(bytes, 0, bytes.length, f);
    t = f.pollTuple();
    assertEquals(expectedFloat.toString(), t.getAttribute("attr"));
  }

  @Test
  public void testDeserializeComposite() throws Exception {
    Long l = 1l;
    byte[] other = ByteUtils.toBytes("other");
    byte[] source = ByteUtils.add(ByteUtils.vintToBytes(ByteUtils.SIZEOF_LONG), ByteUtils.toBytes(l), other);
    Segment suffixedSerde = VintSizePrefixHolder.create(serde);

    TupleDeserializer f = new OneToOneDeserializer(relation);
    setField(f, "tuple", new TupleImpl(relation));
    suffixedSerde.cutAndSet(source, 0, source.length, f);
    Tuple t = f.pollTuple();
    assertEquals(l.toString(), t.getAttribute("attr"));

    Float fv = 0.1f;
    source = ByteUtils.add(ByteUtils.vintToBytes(ByteUtils.SIZEOF_FLOAT), ByteUtils.toBytes(fv), other);
    setField(f, "tuple", new TupleImpl(relation));
    suffixedSerde.cutAndSet(source, 0, source.length, f);
    t = f.pollTuple();
    assertEquals(fv.toString(), t.getAttribute("attr"));
  }
}
