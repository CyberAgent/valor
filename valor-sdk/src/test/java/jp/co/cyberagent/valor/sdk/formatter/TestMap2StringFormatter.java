package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static jp.co.cyberagent.valor.sdk.formatter.FilterFixture.testFilterFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestMap2StringFormatter {

  private final OneToOneDeserializer deserializer;
  private static final MapAttributeType mapType = MapAttributeType
      .create(StringAttributeType.INSTANCE, StringAttributeType.INSTANCE);
  private static final Relation relation = ImmutableRelation.builder()
      .relationId("r").addAttribute("m", false, mapType).build();;
  private final Map2StringFormatter serde;

  static TupleFixture[] getSerdeFixtures() throws JsonProcessingException {
    Map<String, String> m = new LinkedHashMap();
    m.put("k2", "v2");
    m.put("k1", "v1");
    Tuple t = new TupleImpl(relation);
    t.setAttribute("m", m);

    Map<String, String> valWithEqual = new LinkedHashMap();
    valWithEqual.put("k2", "v2");
    valWithEqual.put("k1", ">=v1");
    Tuple tupleWithEqual = new TupleImpl(relation);
    tupleWithEqual.setAttribute("m", valWithEqual);

    Tuple tupleWithEmpty = new TupleImpl(relation);
    tupleWithEmpty.setAttribute("m", new HashMap<>());

    return new TupleFixture[] {
        new TupleFixture(relation, t, ByteUtils.toBytes("k1=v1\tk2=v2")),
        new TupleFixture(relation, tupleWithEqual, ByteUtils.toBytes("k1=>=v1\tk2=v2")),
        new TupleFixture(relation, tupleWithEmpty, ByteUtils.toBytes(""))
    };
  }

  @ParameterizedTest
  @MethodSource("getSerdeFixtures")
  public void testSerde(TupleFixture fixture) throws Exception {
    Map2StringFormatter formatter = new Map2StringFormatter("m");
    TupleFixture.testTupleFixture(formatter, fixture);
  }

  static FilterFixture[] getFilterFixtures() throws JsonProcessingException {
    Map<String, String> m = new LinkedHashMap();
    m.put("k2", "v2");
    m.put("k1", "v1");

    Map<String, String> valWithEqual = new LinkedHashMap();
    valWithEqual.put("k2", "v2");
    valWithEqual.put("k1", ">=v1");
    Tuple tupleWithEqual = new TupleImpl(relation);
    tupleWithEqual.setAttribute("m", valWithEqual);

    return new FilterFixture[] {
        new FilterFixture(
            new EqualOperator("m", mapType, m),
            new CompleteMatchSegment(null, ByteUtils.toBytes("k1=v1\tk2=v2"))),
        new FilterFixture(
            new EqualOperator("m", mapType, valWithEqual),
            new CompleteMatchSegment(null, ByteUtils.toBytes("k1=>=v1\tk2=v2"))),
        new FilterFixture(
            new EqualOperator("m", mapType, new HashMap<>()),
            new CompleteMatchSegment(null, ByteUtils.toBytes("")))
    };
  }

  @ParameterizedTest
  @MethodSource("getFilterFixtures")
  public void testFilter(FilterFixture fixture) throws Exception {
    Map2StringFormatter formatter = new Map2StringFormatter("m");
    testFilterFixture(formatter, fixture);
  }


  public TestMap2StringFormatter() throws Exception {
    serde = new Map2StringFormatter("m");
    deserializer = new OneToOneDeserializer(relation, "", serde);
    setField(deserializer, "tuple", new TupleImpl(relation));
  }


  @SuppressWarnings("unchecked")
  @Test
  public void testValueWithEqualSeparator() throws Exception {
    Map<String, String> m = new LinkedHashMap();
    m.put("k2", "v2");
    m.put("k1", ">=v1");
    Tuple t = new TupleImpl(relation);
    t.setAttribute("m", m);

    byte[] expected = ByteUtils.toBytes("k1=>=v1\tk2=v2");

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    serde.cutAndSet(expected, 0, expected.length, deserializer);
    Map<String, String> actualMap = (Map<String, String>) t.getAttribute("m");
    assertEquals(2, actualMap.size());
    assertEquals(">=v1", actualMap.get("k1"));
    assertEquals("v2", actualMap.get("k2"));
    EqualOperator pred = new EqualOperator("m", mapType, m);

    QuerySerializer querySerializer = mock(QuerySerializer.class);
    serde.accept(querySerializer, Arrays.asList(pred));
    verify(querySerializer).write(null, new CompleteMatchSegment(null, expected));
  }

  @Test
  public void testEmptyMap() throws Exception {
    Map<String, String> m = new HashMap<>();
    Tuple t = new TupleImpl(relation);
    t.setAttribute("m", m);

    byte[] expected = ByteUtils.toBytes("");

    TupleSerializer serializer = mock(TupleSerializer.class);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    serde.cutAndSet(expected, 0, expected.length, deserializer);
    Map<String, String> actualMap = (Map<String, String>) t.getAttribute("m");
    assertEquals(0, actualMap.size());
    EqualOperator pred = new EqualOperator("m", mapType, m);

    QuerySerializer querySerializer = mock(QuerySerializer.class);
    serde.accept(querySerializer, Arrays.asList(pred));
    verify(querySerializer).write(null, new CompleteMatchSegment(null, expected));
  }
}
