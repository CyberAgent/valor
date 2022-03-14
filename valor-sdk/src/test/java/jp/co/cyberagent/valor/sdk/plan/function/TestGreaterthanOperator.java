package jp.co.cyberagent.valor.sdk.plan.function;

import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class TestGreaterthanOperator {

    @Test
    public void testSwappingOperator() {
        AttributeNameExpression attributeNameExpression = new AttributeNameExpression("attr", StringAttributeType.INSTANCE);
        ConstantExpression constantExpression = new ConstantExpression("v");
        GreaterthanOperator greaterthanOperator = new GreaterthanOperator(attributeNameExpression, constantExpression);
        GreaterThanSegment greaterThanSegment = (GreaterThanSegment)greaterthanOperator.buildFilterFragment();

        // 'v > attr' is swapped to 'attr < v'
        GreaterthanOperator swappedGreaterThanOperator = new GreaterthanOperator(constantExpression, attributeNameExpression);
        LessThanSegment lessThanSegment = (LessThanSegment)swappedGreaterThanOperator.buildFilterFragment();

        assertThat(greaterThanSegment.isIncludeBorder(), equalTo(false));
        assertThat(lessThanSegment.isIncludeBorder(), equalTo(false));
    }
}
