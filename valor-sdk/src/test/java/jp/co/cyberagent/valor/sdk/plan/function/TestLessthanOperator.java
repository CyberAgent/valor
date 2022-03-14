package jp.co.cyberagent.valor.sdk.plan.function;

import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;
import org.junit.jupiter.api.Test;

public class TestLessthanOperator {

    @Test
    public void testSwappingOperator() {
        AttributeNameExpression attributeNameExpression = new AttributeNameExpression("attr", StringAttributeType.INSTANCE);
        ConstantExpression constantExpression = new ConstantExpression("v");
        LessthanOperator lessthanOperator = new LessthanOperator(attributeNameExpression, constantExpression);
        LessThanSegment lessThanSegment = (LessThanSegment)lessthanOperator.buildFilterFragment();

        // 'v < attr' is swapped to 'attr > v'
        LessthanOperator swappedlessthanOperator = new LessthanOperator(constantExpression, attributeNameExpression);
        GreaterThanSegment greaterThanSegment = (GreaterThanSegment)swappedlessthanOperator.buildFilterFragment();

        assertThat(lessThanSegment.isIncludeBorder(), equalTo(false));
        assertThat(greaterThanSegment.isIncludeBorder(), equalTo(false));
    }
}
