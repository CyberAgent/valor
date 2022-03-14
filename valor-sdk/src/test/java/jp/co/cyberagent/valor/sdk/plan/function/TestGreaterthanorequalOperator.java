package jp.co.cyberagent.valor.sdk.plan.function;

import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class TestGreaterthanorequalOperator {

    @Test
    public void testSwappingOperator() {
        AttributeNameExpression attributeNameExpression = new AttributeNameExpression("attr", StringAttributeType.INSTANCE);
        ConstantExpression constantExpression = new ConstantExpression("v");
        GreaterthanorequalOperator greaterthanorequalOperator = new GreaterthanorequalOperator(attributeNameExpression, constantExpression);
        GreaterThanSegment greaterThanSegment = (GreaterThanSegment)greaterthanorequalOperator.buildFilterFragment();

        // 'v >= attr' is swapped to 'attr <= v'
        GreaterthanorequalOperator swappedGreaterThanorequalOperator = new GreaterthanorequalOperator(constantExpression, attributeNameExpression);
        LessThanSegment lessThanSegment = (LessThanSegment) swappedGreaterThanorequalOperator.buildFilterFragment();

        assertThat(greaterThanSegment.isIncludeBorder(), equalTo(true));
        assertThat(lessThanSegment.isIncludeBorder(), equalTo(true));
    }
}
