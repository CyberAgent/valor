package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static jp.co.cyberagent.valor.sdk.formatter.FilterFixture.testFilterFixture;
import static org.junit.jupiter.api.Assertions.assertEquals;

import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestReverseLongFormatter {

  private static final String ATTR_NAME = "attr";
  private static final ReverseLongFormatter serde
      = new ReverseLongFormatter.Factory().create(ATTR_NAME);
  private static final Relation relation = ImmutableRelation.builder()
      .relationId("test").addAttribute(ATTR_NAME, true, LongAttributeType.INSTANCE).build();

  @Test
  public void testRecord() throws Exception {
    long v1 = 1l;
    byte[] serialized = serde.serialize(v1, LongAttributeType.INSTANCE);
    assertEquals(Long.MAX_VALUE - v1, ByteUtils.toLong(serialized));

    TupleDeserializer deserializer = new OneToOneDeserializer(relation);
    setField(deserializer, "tuple", new TupleImpl(relation));
    serde.cutAndSet(serialized, 0, serialized.length, deserializer);
    Tuple t = deserializer.pollTuple();
    assertEquals(v1, t.getAttribute(ATTR_NAME));
  }

  static byte[] value = ByteUtils.toBytes(Long.MAX_VALUE - 1l);
  static EqualOperator eop = new EqualOperator(ATTR_NAME, LongAttributeType.INSTANCE, 1l);
  static GreaterthanOperator gtop
      = new GreaterthanOperator(ATTR_NAME, LongAttributeType.INSTANCE, 1l);
  static LessthanOperator ltop
      = new LessthanOperator(ATTR_NAME, LongAttributeType.INSTANCE, 1l);
  static GreaterthanorequalOperator gteqop
      = new GreaterthanorequalOperator(ATTR_NAME, LongAttributeType.INSTANCE, 1l);
  static LessthanorequalOperator lteqop
      = new LessthanorequalOperator(ATTR_NAME, LongAttributeType.INSTANCE, 1l);

  public static FilterFixture[] fixtures = {
      new FilterFixture(eop, new CompleteMatchSegment(eop, value)),
      new FilterFixture(gtop, new LessThanSegment(gtop, value, false)),
      new FilterFixture(ltop, new GreaterThanSegment(ltop, value, false)),
      new FilterFixture(gteqop, new LessThanSegment(gtop, value, true)),
      new FilterFixture(lteqop, new GreaterThanSegment(ltop, value, true))
  };

  static FilterFixture[] getFixtures() {
    return fixtures;
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testFilter(FilterFixture fixture) throws ValorException {
    testFilterFixture(serde, fixture);
  }

}
