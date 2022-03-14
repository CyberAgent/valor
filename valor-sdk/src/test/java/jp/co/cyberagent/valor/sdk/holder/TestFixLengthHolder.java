package jp.co.cyberagent.valor.sdk.holder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
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
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestFixLengthHolder {

  private Relation relation = ImmutableRelation.builder().relationId("test").addAttribute("long",
      true, LongAttributeType.INSTANCE).build();
  private Formatter formatter = AttributeValueFormatter.create("long");
  private Holder serde = FixedLengthHolder.create(ByteUtils.SIZEOF_LONG, formatter);

  @Test
  public void testSerialize() throws Exception {
    Long v = 1l;
    Tuple t = new TupleImpl(relation);
    t.setAttribute("long", v);
    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(EqualBytes.equalBytes(ByteUtils.toBytes(v))));
  }

  @Test
  public void testDeserialize() throws Exception {
    Long v = 1l;
    TupleDeserializer deserializer = new OneToOneDeserializer(relation, FieldLayout.of("", serde));
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    byte[] bytes = ByteUtils.toBytes(v);
    serde.cutAndSet(bytes, 0, bytes.length, deserializer);
    Tuple tuple = deserializer.pollTuple();
    assertThat(tuple.getAttribute("long"), equalTo(1l));
  }
}
