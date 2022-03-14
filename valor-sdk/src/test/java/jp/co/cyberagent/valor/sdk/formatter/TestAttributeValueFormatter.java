package jp.co.cyberagent.valor.sdk.formatter;

import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestAttributeValueFormatter {

  private static final String ATTR = "a";

  private static TupleFixture[] getFixtures() {
    Relation relation = ImmutableRelation.builder().relationId("testAttributeValue")
        .addAttribute("a", true, StringAttributeType.INSTANCE)
        .build();
    Tuple t = new TupleImpl(relation);
    t.setAttribute("a", "");
    return new TupleFixture[] {
        new TupleFixture(relation, t, new byte[0])
    };
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testSerde(TupleFixture fixture) throws Exception {
    AttributeValueFormatter element = AttributeValueFormatter.create(ATTR);
    TupleFixture.testTupleFixture(element, fixture);
  }
}
