package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.NumberAttributeType;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestNumber2StringFormatter {

  static final String ATTR_NAME = "number";

  private Number2StringFormatter serde = new Number2StringFormatter(ATTR_NAME);
  private Relation relation =
      ImmutableRelation.builder().relationId("test")
          .addAttribute(ATTR_NAME, true, NumberAttributeType.INSTANCE).build();

  @Test
  public void testSerialize() throws Exception {
    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR_NAME, 1l);
    byte[] expected = ByteUtils.toBytes(Long.toString(1l));

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    t.setAttribute(ATTR_NAME, 0.1f);
    expected = ByteUtils.toBytes(Float.toString(0.1f));

    serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }

  @Test
  public void testDeserialize() throws Exception {
    Long lv = 100l;
    TupleDeserializer f = new OneToOneDeserializer(relation, "", serde);
    setField(f, "tuple", new TupleImpl(relation));
    byte[] bytes = ByteUtils.toBytes(lv.toString());
    serde.cutAndSet(bytes, 0, bytes.length, f);
    Tuple t = f.pollTuple();
    assertEquals(100l, t.getAttribute("number"));

    Float fv = 0.01f;
    bytes = ByteUtils.toBytes(fv.toString());
    setField(f, "tuple", new TupleImpl(relation));
    serde.cutAndSet(bytes, 0, bytes.length, f);
    t = f.pollTuple();
    assertEquals(0.01f, t.getAttribute("number"));
  }
}
