package jp.co.cyberagent.valor.sdk.formatter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestJsonFormatter {

  static ObjectMapper objectMapper = new ObjectMapper();

  static TupleFixture[] getFixtures() throws JsonProcessingException {
    Relation relation = ImmutableRelation.builder().relationId("r")
        .addAttribute("k", true, StringAttributeType.INSTANCE)
        .addAttribute("v", false, IntegerAttributeType.INSTANCE)
        .build();
    Tuple t = new TupleImpl(relation);
    t.setAttribute("k", "key");
    t.setAttribute("v", 100);
    String expected = objectMapper.writeValueAsString(new HashMap(){
      {
        put("k", "key");
        put("v", 100);
      }
    });
    return new TupleFixture[] {
        new TupleFixture(relation, t, expected.getBytes(StandardCharsets.UTF_8))
    };
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testSerde(TupleFixture fixture) throws Exception {
    JsonFormatter formatter = new JsonFormatter(new HashMap(){
      {
        put("attrs", Arrays.asList("k", "v"));
      }
    });
    TupleFixture.testTupleFixture(formatter, fixture);
  }

}
