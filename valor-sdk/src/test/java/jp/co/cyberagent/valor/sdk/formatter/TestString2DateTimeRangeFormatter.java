package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.util.HashMap;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import org.junit.jupiter.api.Test;

public class TestString2DateTimeRangeFormatter {

  private static final String ATTR_NAME = "attr";

  private Relation relation;
  private Formatter serde;

  public TestString2DateTimeRangeFormatter() {
    relation = ImmutableRelation.builder().relationId("test").addAttribute(ATTR_NAME, true,
        StringAttributeType.INSTANCE).build();
    serde = new String2DateTimeFrameFormatter.Factory()
        .create(new HashMap(){{
          put(AbstractAttributeValueFormatter.ATTRIBUTE_NAME_PROPKEY, ATTR_NAME);
        }});
  }

  @Test
  public void testToBytesFromBytesMonthly() throws Exception {
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(ATTR_NAME, "2013-12");
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.put(String2DateTimeFrameFormatter.FrameType.MONTHLY.value());
    buf.putShort((short) 2013);
    buf.put((byte) 12);
    buf.put((byte) -1);
    buf.put((byte) -1);
    buf.put((byte) -1);
    buf.put((byte) -1);
    byte[] expected = buf.array();

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, tuple);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", serde);
    setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(expected, 0, expected.length, deserializer);
    assertEquals("2013-12", deserializer.pollTuple().getAttribute(ATTR_NAME));
  }

  @Test
  public void testToBytesFromBytesDaily() throws Exception {
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(ATTR_NAME, "2013-12-06");
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.put(String2DateTimeFrameFormatter.FrameType.DAILY.value());
    buf.putShort((short) 2013);
    buf.put((byte) 12);
    buf.put((byte) 6);
    buf.put((byte) -1);
    buf.put((byte) -1);
    buf.put((byte) -1);
    byte[] expected = buf.array();

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, tuple);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", serde);
    setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(expected, 0, expected.length, deserializer);
    assertEquals("2013-12-06", deserializer.pollTuple().getAttribute(ATTR_NAME));
  }

  @Test
  public void testToBytesFromBytesHourly() throws Exception {
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(ATTR_NAME, "2013-12-06 17");
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.put(String2DateTimeFrameFormatter.FrameType.HOURLY.value());
    buf.putShort((short) 2013);
    buf.put((byte) 12);
    buf.put((byte) 6);
    buf.put((byte) 17);
    buf.put((byte) -1);
    buf.put((byte) -1);
    byte[] expected = buf.array();

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, tuple);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", serde);
    setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(expected, 0, expected.length, deserializer);
    assertEquals("2013-12-06 17", deserializer.pollTuple().getAttribute(ATTR_NAME));
  }

  @Test
  public void testToBytesFromBytesMinutely() throws Exception {
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(ATTR_NAME, "2013-12-06 17:45");

    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.put(String2DateTimeFrameFormatter.FrameType.MINUTELY.value());
    buf.putShort((short) 2013);
    buf.put((byte) 12);
    buf.put((byte) 6);
    buf.put((byte) 17);
    buf.put((byte) 45);
    buf.put((byte) -1);
    byte[] expected = buf.array();

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, tuple);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", serde);
    setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(expected, 0, expected.length, deserializer);
    assertEquals("2013-12-06 17:45", deserializer.pollTuple().getAttribute(ATTR_NAME));
  }

  @Test
  public void testToBytesFromBytesSecondly() throws Exception {
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(ATTR_NAME, "2013-12-06 17:45:30");
    ByteBuffer buf = ByteBuffer.allocate(8);
    buf.put(String2DateTimeFrameFormatter.FrameType.SECONDLY.value());
    buf.putShort((short) 2013);
    buf.put((byte) 12);
    buf.put((byte) 6);
    buf.put((byte) 17);
    buf.put((byte) 45);
    buf.put((byte) 30);
    byte[] expected = buf.array();

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, tuple);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", serde);
    setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(expected, 0, expected.length, deserializer);
    assertEquals("2013-12-06 17:45:30", deserializer.pollTuple().getAttribute(ATTR_NAME));
  }
}
