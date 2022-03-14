package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestNullableStringFormatter {

  private static final String ATTR_NAME = "attr";
  private static final byte NUL = 0x00;
  private static final String NON_NUL = "a";

  private Formatter serde = new NullableStringFormatter(ATTR_NAME);
  private Relation relation =
      ImmutableRelation.builder().relationId("test").addAttribute(ATTR_NAME, true,
          StringAttributeType.INSTANCE).build();

  @Test
  public void testSerializeNull() throws Exception {
    Tuple t = new TupleImpl(relation);
    TupleSerializer serializer = mock(TupleSerializer.class);
    t.setAttribute(ATTR_NAME, null);
    serde.accept(serializer, t);

    byte[] expectedNullBytes = ByteUtils.toBytes(NUL);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expectedNullBytes)));
  }

  @Test
  public void testSerializeNonNull() throws Exception {
    Tuple t = new TupleImpl(relation);
    TupleSerializer serializer = mock(TupleSerializer.class);
    t.setAttribute(ATTR_NAME, NON_NUL);
    serde.accept(serializer, t);

    byte[] expectedNonNullBytes = NON_NUL.getBytes(StringAttributeType.CHARSET_NAME);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expectedNonNullBytes)));
  }

  @Test
  public void testDeserializeNull() throws Exception {
    TupleDeserializer f = new OneToOneDeserializer(relation);
    setField(f, "tuple", new TupleImpl(relation));
    byte[] bytes = ByteUtils.toBytes(NUL);

    serde.cutAndSet(bytes, 0, bytes.length, f);
    Tuple t = f.pollTuple();
    assertEquals(null, t.getAttribute(ATTR_NAME));
  }

  @Test
  public void testDeserializeNonNull() throws Exception {
    TupleDeserializer f = new OneToOneDeserializer(relation);
    setField(f, "tuple", new TupleImpl(relation));
    byte[] bytes = NON_NUL.getBytes(StringAttributeType.CHARSET_NAME);

    serde.cutAndSet(bytes, 0, bytes.length, f);
    Tuple t = f.pollTuple();
    assertEquals(NON_NUL, t.getAttribute(ATTR_NAME));
  }

  @Test
  public void testFilter() throws Exception {
    String v = null;
    EqualOperator c = new EqualOperator(ATTR_NAME, StringAttributeType.INSTANCE, v);
    QuerySerializer serializer = mock(QuerySerializer.class);
    serde.accept(serializer, Arrays.asList(c));
    verify(serializer).write(null, new CompleteMatchSegment(null, ByteUtils.toBytes(NUL)));
  }
}
