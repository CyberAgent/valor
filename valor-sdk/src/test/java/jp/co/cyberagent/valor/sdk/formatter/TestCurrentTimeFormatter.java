package jp.co.cyberagent.valor.sdk.formatter;

import static org.mockito.Mockito.doReturn;

import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

public class TestCurrentTimeFormatter {

  private static final String ATTR = "a";
  private static final long TIME = 1L;

  private static TupleFixture[] getFixtures() {
    final Relation relation = ImmutableRelation.builder().relationId("testAttributeValue")
        .addAttribute(ATTR, true, LongAttributeType.INSTANCE)
        .build();
    final Tuple tuple = new TupleImpl(relation);
    final Tuple desTuple = new TupleImpl(relation);
    desTuple.setAttribute(ATTR, TIME);
    return new TupleFixture[] {
        new TupleFixture(relation, tuple, ByteUtils.toBytes(TIME), desTuple)
    };
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testSerde(TupleFixture fixture) throws Exception {
    final CurrentTimeFormatter element = Mockito.spy(new CurrentTimeFormatter(ATTR));
    doReturn(TIME).when(element).getCurrentTime();

    TupleFixture.testTupleFixture(element, fixture);
  }
}
