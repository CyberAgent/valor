package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;


public class TestMd5AttributeFormatter {

  static final String ATTR1 = "attr1";
  static final String ATTR2 = "attr2";

  private static final int LENGTH = 5;
  private Relation relation = ImmutableRelation.builder().relationId("test")
      .addAttribute(ATTR1, true, StringAttributeType.INSTANCE)
      .addAttribute(ATTR2, true, StringAttributeType.INSTANCE)
      .build();

  @Test
  public void testSerDes() throws Exception {
    String prehash = "prehash";
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(ATTR1, prehash);
    byte[] expected = new byte[] {-63, -110, 50, 85, 85, -12, 3, 86, 120, 89, 23, -5, 9, 95, -126, -72};

    expected = Arrays.copyOf(expected, LENGTH);

    TupleSerializer serializer = mock(TupleSerializer.class);
    Md5AttributeFormatter element = Md5AttributeFormatter.create(LENGTH,ATTR1);
    element.accept(serializer, tuple);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", element);
    setField(deserializer, "tuple", new TupleImpl(relation));
    byte[] source = ByteUtils.add(expected, ByteUtils.toBytes(prehash));
    int l = element.cutAndSet(source, 0, source.length, deserializer);
    assertEquals(LENGTH, l);
    tuple = deserializer.pollTuple();
    // hash formatter cannot keep its value;
    assertNull(tuple.getAttribute(ATTR1));
    setField(deserializer, "tuple", new TupleImpl(relation));
    new AttributeValueFormatter(ATTR1).cutAndSet(source, l, source.length - l, deserializer);
    tuple = deserializer.pollTuple();
    assertEquals(prehash, tuple.getAttribute(ATTR1));

    // test scan
    EqualOperator ep = new EqualOperator(ATTR1, StringAttributeType.INSTANCE, prehash);
    QuerySerializer qserializer = mock(QuerySerializer.class);
    element.accept(qserializer, Arrays.asList(ep));
    verify(qserializer).write(null, new CompleteMatchSegment(null, expected));
  }

  @Test
  public void testMultiAttributes() throws Exception {
    String prehash1 = "prehash1";
    String prehash2 = "prehash2";
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(ATTR1, prehash1);
    tuple.setAttribute(ATTR2, prehash2);
    byte[] expected =
        DigestUtils.md5(ByteUtils.add(prehash1.getBytes(), prehash2.getBytes()));
    expected = Arrays.copyOf(expected, LENGTH);

    // test serialize
    TupleSerializer serializer = mock(TupleSerializer.class);
    Md5AttributeFormatter element = Md5AttributeFormatter.create(LENGTH, ATTR1, ATTR2);
    element.accept(serializer, tuple);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    // test deserialize
    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", element);
    setField(deserializer, "tuple", new TupleImpl(relation));
    byte[] source = ByteUtils.add(expected, ByteUtils.toBytes(prehash1));
    int l = element.cutAndSet(source, 0, source.length, deserializer);
    assertEquals(LENGTH, l);
    tuple = deserializer.pollTuple();
    // hash formatter cannot keep its value;
    assertNull(tuple.getAttribute(ATTR1));
    setField(deserializer, "tuple", new TupleImpl(relation));
    new AttributeValueFormatter(ATTR1).cutAndSet(source, l,source.length - l, deserializer);
    tuple = deserializer.pollTuple();
    assertEquals(prehash1, tuple.getAttribute(ATTR1));

    // test scan
    EqualOperator eq1 = new EqualOperator(ATTR1, StringAttributeType.INSTANCE, prehash1);
    EqualOperator eq2 = new EqualOperator(ATTR2, StringAttributeType.INSTANCE, prehash2);
    QuerySerializer qserializer = mock(QuerySerializer.class);
    element.accept(qserializer, Arrays.asList(eq1, eq2));
    verify(qserializer).write(null, new CompleteMatchSegment(null, expected));

    qserializer = mock(QuerySerializer.class);
    element.accept(qserializer, Arrays.asList(eq1));
    verify(qserializer).write(null, TrueSegment.INSTANCE);
  }
}
