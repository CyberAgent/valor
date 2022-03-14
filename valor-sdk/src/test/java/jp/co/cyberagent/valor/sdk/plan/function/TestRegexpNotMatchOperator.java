package jp.co.cyberagent.valor.sdk.plan.function;

import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRegexpNotMatchOperator {

    @Test
    public void testUnlikeOperator() {
        AttributeNameExpression attributeNameExpression = new AttributeNameExpression("attr", StringAttributeType.INSTANCE);
        ConstantExpression constantExpression = new ConstantExpression(null);

        RegexpNotMatchOperator regexpNotMatchOperator = new RegexpNotMatchOperator(attributeNameExpression, constantExpression);
        assertFalse(regexpNotMatchOperator.test("100", "1.*"));
        assertTrue(regexpNotMatchOperator.test("200", "1.*"));
    }
}
