package jp.co.cyberagent.valor.sdk.plan.function;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestRegexpOperator {

    @ParameterizedTest
    @CsvSource(value = {
        "a.*, aaa, true",
        "a.*, bbb, false",
        "a.*, null, null",
    }, nullValues = {"null"})
    public void test(String regexp, String value, Boolean result) {
        String attr = "k";
        Relation rel = ImmutableRelation.builder().relationId("r")
            .addAttribute(attr, true, StringAttributeType.INSTANCE).build();
        Tuple t = new TupleImpl(rel);
        t.setAttribute(attr, value);

        AttributeNameExpression attributeNameExpression
            = new AttributeNameExpression(attr, StringAttributeType.INSTANCE);
        ConstantExpression regexpExpression = new ConstantExpression(regexp);
        RegexpOperator op = new RegexpOperator(attributeNameExpression, regexpExpression);

        // test apply()
        assertEquals(op.apply(t), result);

        // test test()
        assertEquals(op.test(t), Boolean.TRUE.equals(result));
    }

    @ParameterizedTest
    @MethodSource("getMapPatternFixture")
    public void testMap(MapPatternFixture fixture) {
        String attr = "m";
        MapAttributeType mt = MapAttributeType.create(
            StringAttributeType.INSTANCE, StringAttributeType.INSTANCE);
        Relation rel = ImmutableRelation.builder().relationId(attr)
            .addAttribute(attr, true, mt).build();
        Tuple t = new TupleImpl(rel);
        t.setAttribute(attr, fixture.value);

        AttributeNameExpression attributeNameExpression = new AttributeNameExpression(attr, mt);
        ConstantExpression regexpExpression = new ConstantExpression(fixture.pattern);
        RegexpOperator op = new RegexpOperator(attributeNameExpression, regexpExpression);

        // test apply()
        assertEquals(op.apply(t), fixture.result);

        // test test()
        assertEquals(op.test(t), Boolean.TRUE.equals(fixture.result));
    }

    static MapPatternFixture[] getMapPatternFixture() {
        Map<String, String> pattern1 = Collections.singletonMap("k", "a.*c");
        MapPatternFixture fixture1 = new MapPatternFixture(
            pattern1, Collections.singletonMap("k", "abc"), true);
        MapPatternFixture fixture2 = new MapPatternFixture(
            pattern1, Collections.singletonMap("k", "bc"), false);
        MapPatternFixture fixture3 = new MapPatternFixture(
            pattern1, Collections.singletonMap("k", "ab"), false);
        MapPatternFixture fixture4 = new MapPatternFixture(
            pattern1, Collections.singletonMap("k1", "abc"), false);
        Map<String, String> v5 = new HashMap<>();
        v5.put("k", "abc");
        v5.put("l", "abc");
        MapPatternFixture fixture5 = new MapPatternFixture(pattern1, v5, false);
        return new MapPatternFixture[]{ fixture1, fixture2, fixture3, fixture4, fixture5 };

    }

    static class MapPatternFixture {
        public final Map<String, String> pattern;
        public final Map<String, String> value;
        public final Boolean result;

        MapPatternFixture(Map<String, String> pattern, Map<String, String> value, Boolean result) {
            this.pattern = pattern;
            this.value = value;
            this.result = result;
        }
    }
}
