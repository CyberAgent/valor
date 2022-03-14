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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.udf.UdfMapKeys;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.UdfExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import jp.co.cyberagent.valor.spi.util.MurmurHash3;
import org.junit.jupiter.api.Test;

public class TestMurmur3MapKeyFormatter {

  static final String MAP_ATTR = "m";
  static final AttributeType MAP_TYPE
      = MapAttributeType.create(StringAttributeType.INSTANCE, StringAttributeType.INSTANCE);

  private Relation relation = ImmutableRelation.builder().relationId("test")
      .addAttribute(MAP_ATTR,true, MAP_TYPE)
      .build();

  @Test
  public void testSerDes() throws Exception {
    String prehash = "prehash";
    Map<String, String> v1 = Collections.singletonMap(prehash, "noop1");
    Map<String, String> v2 = Collections.singletonMap(prehash, "noop2");
    Tuple tuple1 = new TupleImpl(relation);
    tuple1.setAttribute(MAP_ATTR, v1);
    Tuple tuple2 = new TupleImpl(relation);
    tuple2.setAttribute(MAP_ATTR, v2);
    byte[] expected = new byte[] {-7, 93, -115, -4};

    Murmur3MapKeyFormatter element = Murmur3MapKeyFormatter.create(MAP_ATTR);

    // test serialize
    TupleSerializer serializer = mock(TupleSerializer.class);
    element.accept(serializer, tuple1);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    serializer = mock(TupleSerializer.class);
    element.accept(serializer, tuple2);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    // test deserialize
    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", element);
    setField(deserializer, "tuple", new TupleImpl(relation));
    byte[] source = ByteUtils.add(expected, MAP_TYPE.serialize(v1));
    int l = element.cutAndSet(source, 0, source.length, deserializer);
    assertEquals(ByteUtils.SIZEOF_INT, l);
    Tuple tuple = deserializer.pollTuple();
    // hash formatter cannot keep its value;
    assertNull(tuple.getAttribute(MAP_ATTR));
    setField(deserializer, "tuple", new TupleImpl(relation));
    new AttributeValueFormatter(MAP_ATTR).cutAndSet(source, l, source.length - l, deserializer);
    tuple = deserializer.pollTuple();
    assertEquals(v1, tuple.getAttribute(MAP_ATTR));

    // test scan
    Stream.of(
        new EqualOperator(MAP_ATTR, MAP_TYPE, v1),
        new EqualOperator(MAP_ATTR, MAP_TYPE, v2),
        new EqualOperator(
            new UdfExpression(
                new UdfMapKeys(), Arrays.asList(new AttributeNameExpression(MAP_ATTR, MAP_TYPE))),
            new ConstantExpression(Arrays.asList(prehash)))
    ).forEach(p -> {
      QuerySerializer qserializer = mock(QuerySerializer.class);
      element.accept(qserializer, Arrays.asList(p));
      verify(qserializer).write(null, new CompleteMatchSegment(null, expected));
    });
  }

  @Test
  public void testMultipleKeys() throws Exception {
    String prehash1 = "prehash1";
    String prehash2 = "prehash2";
    Map<String, String> v = new HashMap<String, String>() {
      {
        put(prehash1, "test1");
        put(prehash2, "test2");
      }
    };
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(MAP_ATTR, v);
    byte[] expected = ByteUtils.toBytes(
        MurmurHash3.hash(ByteUtils.add(prehash1.getBytes(), prehash2.getBytes())));

    // test serialize
    TupleSerializer serializer = mock(TupleSerializer.class);
    Murmur3MapKeyFormatter element = Murmur3MapKeyFormatter.create(MAP_ATTR);
    element.accept(serializer, tuple);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    // test deserialize
    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", element);
    setField(deserializer, "tuple", new TupleImpl(relation));
    byte[] source = ByteUtils.add(expected, MAP_TYPE.serialize(v));
    int l = element.cutAndSet(source, 0, source.length, deserializer);
    assertEquals(ByteUtils.SIZEOF_INT, l);
    tuple = deserializer.pollTuple();
    // hash formatter cannot keep its value;
    assertNull(tuple.getAttribute(MAP_ATTR));
    setField(deserializer, "tuple", new TupleImpl(relation));
    new AttributeValueFormatter(MAP_ATTR).cutAndSet(source, l, source.length - l, deserializer);
    tuple = deserializer.pollTuple();
    assertEquals(v, tuple.getAttribute(MAP_ATTR));

    // test scan
    EqualOperator eq = new EqualOperator(MAP_ATTR, MAP_TYPE, v);
    QuerySerializer qserializer = mock(QuerySerializer.class);
    element.accept(qserializer, Arrays.asList(eq));
    verify(qserializer).write(null, new CompleteMatchSegment(null, expected));
  }

}
