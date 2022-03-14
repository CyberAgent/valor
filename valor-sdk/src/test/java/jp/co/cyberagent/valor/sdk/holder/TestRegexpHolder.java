package jp.co.cyberagent.valor.sdk.holder;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.sdk.EqualBytes;
import jp.co.cyberagent.valor.sdk.ReflectionUtil;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestRegexpHolder {

  private static final String ATTR = "attr";
  private Formatter formatter = AttributeValueFormatter.create(ATTR);
  private Relation relation = ImmutableRelation.builder().relationId("test").addAttribute(ATTR,
      true, StringAttributeType.INSTANCE).build();

  @Test
  public void testSerialize() throws Exception {
    Tuple t = new TupleImpl(relation);
    Holder serde = RegexpHolder.create("[\\d]{4}-[0-9]{2}[a-z]?", formatter);

    TupleSerializer serializer = mock(TupleSerializer.class);
    t.setAttribute(ATTR, "1111-22a");
    serde.accept(serializer, t);
    verify(serializer).write(isNull(), argThat(EqualBytes.equalBytes(ByteUtils.toBytes("1111-22a"))));

    t.setAttribute(ATTR, "x");
    try {
      serde.accept(serializer, t);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerdeException);
    }

    t.setAttribute(ATTR, "1111-22a-bc");
    try {
      serde.accept(serializer, t);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerdeException);
    }
  }

  @Test
  public void testDeserialize() throws Exception {
    Holder serde = RegexpHolder.create("[\\d]{4}-[0-9]{2}[a-z]?", formatter);
    TupleDeserializer deserializer = new OneToOneDeserializer(relation);
    ReflectionUtil.setField(deserializer, "tuple", new TupleImpl(relation));
    byte[] bytes = ByteUtils.toBytes("1111-22a-abc");

    int l = serde.cutAndSet(bytes, 0, bytes.length, deserializer);
    Tuple t = deserializer.pollTuple();
    assertEquals("1111-22a", t.getAttribute("attr"));
    byte[] r = ByteUtils.copy(bytes, l, 4);
    assertArrayEquals(ByteUtils.toBytes("-abc"), r);


    bytes = ByteUtils.toBytes("xbc");
    try {
      serde.cutAndSet(bytes, 0, bytes.length, deserializer);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerdeException);
    }

    bytes = ByteUtils.toBytes("xbc-1111-22a-abc");
    try {
      serde.cutAndSet(bytes, 0, bytes.length, deserializer);
      fail();
    } catch (Exception e) {
      assertTrue(e instanceof SerdeException);
    }
  }

  @Test
  public void testFilterSlice() throws Exception {
    Holder serde = RegexpHolder.create("[a-z]{4}", formatter);
    List<PrimitivePredicate> conjunction = Arrays.asList(
        new EqualOperator("attr", StringAttributeType.INSTANCE, "a"));
    QuerySerializer serializer = mock(QuerySerializer.class);
    serde.accept(serializer, conjunction);
    verify(serializer).write(null, new CompleteMatchSegment(null, ByteUtils.toBytes("a")));
  }
}
