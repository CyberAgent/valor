package jp.co.cyberagent.valor.sdk.plan.function;

import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class TestLessthanorequalOperator {

    @Test
    public void testSwappingOperator() {
        AttributeNameExpression attributeNameExpression = new AttributeNameExpression("attr", StringAttributeType.INSTANCE);
        ConstantExpression constantExpression = new ConstantExpression("v");
        LessthanorequalOperator lessthanorequalOperator = new LessthanorequalOperator(attributeNameExpression, constantExpression);
        LessThanSegment lessThanSegment = (LessThanSegment)lessthanorequalOperator.buildFilterFragment();

        // 'v <= attr' is swapped to 'attr >= v'
        LessthanorequalOperator swappedLessthanorequalOperator = new LessthanorequalOperator(constantExpression, attributeNameExpression);
        GreaterThanSegment greaterThanSegment = (GreaterThanSegment)swappedLessthanorequalOperator.buildFilterFragment();

        assertThat(lessThanSegment.isIncludeBorder(), equalTo(true));
        assertThat(greaterThanSegment.isIncludeBorder(), equalTo(true));
    }
}
