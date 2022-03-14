package jp.co.cyberagent.valor.sdk.formatter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestJqFormatter {

  static ObjectMapper objectMapper = new ObjectMapper();

  static class JqFixture extends TupleFixture {

    public final JqFormatter formatter;

    public JqFixture(Relation relation, String jq, String json, Tuple tuple) {
      super(relation, null, json.getBytes(StandardCharsets.UTF_8), tuple);
      this.formatter = JqFormatter.create(jq);
    }
  }

  static JqFixture[] getFixtures() throws JsonProcessingException {
    Relation relation = ImmutableRelation.builder().relationId("r")
        .addAttribute("k", true, StringAttributeType.INSTANCE)
        .addAttribute("v", false, IntegerAttributeType.INSTANCE)
        .build();
    Tuple t = new TupleImpl(relation);
    t.setAttribute("k", "key");
    t.setAttribute("v", 100);

    MapAttributeType mapType = MapAttributeType.create(
        StringAttributeType.INSTANCE, IntegerAttributeType.INSTANCE);
    Relation relWitMap = ImmutableRelation.builder().relationId("rm")
        .addAttribute("k", true, StringAttributeType.INSTANCE)
        .addAttribute("m", false, mapType)
        .build();
    Map<String, Integer> m = new HashMap<>();
    m.put("k", 100);
    Tuple mt = new TupleImpl(relWitMap);
    mt.setAttribute("k", "key");
    mt.setAttribute("m", m);
    return new JqFixture[] {
        new JqFixture(relation, "{k:.foo, v:.bar.hoge}", "{\"foo\":\"key\",\"bar\":{\"hoge\":100}}", t),
        new JqFixture(relation, "{k:.foo, v:.bar.hoge | tonumber }",
            "{\"foo\":\"key\",\"bar\":{\"hoge\":\"100\"}}", t),
        new JqFixture(relWitMap, "{k:.foo, m:{k:.bar.hoge}}", "{\"foo\":\"key\",\"bar\":{\"hoge\":100}}", mt)
    };
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testSerde(JqFixture fixture) throws Exception {
    JqFormatter formatter = fixture.formatter;
    TupleFixture.testTupleFixture(formatter, fixture);
  }

}
