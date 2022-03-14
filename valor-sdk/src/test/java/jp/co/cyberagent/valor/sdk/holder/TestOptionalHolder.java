package jp.co.cyberagent.valor.sdk.holder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.anyVararg;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import jp.co.cyberagent.valor.sdk.EqualBytes;
import jp.co.cyberagent.valor.sdk.ReflectionUtil;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestOptionalHolder {
  private static final String ATTR = "attr";
  private final Formatter formatter = AttributeValueFormatter.create(ATTR);
  private final Relation relation =
      ImmutableRelation.builder()
          .relationId("test")
          .addAttribute(ATTR, true, LongAttributeType.INSTANCE)
          .build();

  @Test
  public void testSerializeAndDeserializeWithFormatter() throws Exception {
    Holder serde = OptionalHolder.create(formatter);
    Long value = 10000L;
    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR, value);

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(EqualBytes.equalBytes(ByteUtils.toBytes(value))));

    byte[] serialized = ByteUtils.toBytes(value);
    TupleDeserializer deserializer = new OneToOneDeserializer(relation);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(serialized, 0, serialized.length, deserializer);
    t = deserializer.pollTuple();
    assertEquals(value, t.getAttribute(ATTR));
  }

  @Test
  public void testSerializeAndDeserializeEmptyWithFormatter() throws Exception {
    Holder serde = OptionalHolder.create(formatter);
    Long value = null;
    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR, value);

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer, never()).write(anyString(), anyVararg());

    byte[] serialized = {};
    TupleDeserializer deserializer = new OneToOneDeserializer(relation);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(serialized, 0, serialized.length, deserializer);
    t = deserializer.pollTuple();
    assertEquals(value, t.getAttribute(ATTR));
  }

  @Test
  public void testSerializeAndDeserializeWithHolder() throws Exception {
    Holder holder = SuffixHolder.create("-", formatter);
    Holder serde = OptionalHolder.create(holder);
    Long value = 10000L;

    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR, value);

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer)
        .write(
            isNull(String.class),
            argThat(EqualBytes.equalBytes(ByteUtils.toBytes(value))),
            argThat(EqualBytes.equalBytes(ByteUtils.toBytes("-"))));

    byte[] serialized = ByteUtils.add(ByteUtils.toBytes(value), ByteUtils.toBytes("-"));
    TupleDeserializer deserializer = new OneToOneDeserializer(relation);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(serialized, 0, serialized.length, deserializer);
    t = deserializer.pollTuple();
    assertEquals(value, t.getAttribute(ATTR));
  }

  @Test
  public void testSerializeAndDeserializeEmptyWithHolder() throws Exception {
    Holder holder = SuffixHolder.create("-", formatter);
    Holder serde = OptionalHolder.create(holder);
    Long value = null;

    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR, value);

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer, never()).write(anyString(), anyVararg());

    byte[] serialized = {};
    TupleDeserializer deserializer = new OneToOneDeserializer(relation);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(serialized, 0, serialized.length, deserializer);
    t = deserializer.pollTuple();
    assertEquals(value, t.getAttribute(ATTR));
  }
}
