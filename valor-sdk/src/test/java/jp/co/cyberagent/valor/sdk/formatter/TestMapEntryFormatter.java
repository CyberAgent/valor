package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.formatter.TupleFixture.testTupleFixture;

import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestMapEntryFormatter {

  private static final String ATTR = "m";
  private static final String KEY = "k";

  private static TupleFixture[] getFixtures() {
    MapAttributeType mapType = MapAttributeType.create(
        StringAttributeType.INSTANCE, StringAttributeType.INSTANCE);
    Relation relation = ImmutableRelation.builder().relationId("testMapEntry")
        .addAttribute(ATTR, true, mapType)
        .build();

    Map<String, String> fullMap = new HashMap<>();
    fullMap.put(KEY, "v");
    fullMap.put("l", "w");
    Map<String, String> filteredMap = new HashMap<>();
    filteredMap.put(KEY, "v");

    Tuple org = new TupleImpl(relation);
    org.setAttribute(ATTR, fullMap);
    Tuple deserialized = new TupleImpl(relation);
    deserialized.setAttribute(ATTR, filteredMap);
    return new TupleFixture[] {
        new TupleFixture(relation, org, ByteUtils.toBytes("v"), deserialized)
    };
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testSerde(TupleFixture fixture) throws Exception {
    MapEntryFormatter formatter = MapEntryFormatter.create(ATTR, KEY);
    testTupleFixture(formatter, fixture);
  }
}
