package jp.co.cyberagent.valor.sdk.plan.function;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.Test;

public class TestIsNotNullOperator {
  final String ATTR_NAME = "k";
  final Expression ATTR_EXP = new AttributeNameExpression(ATTR_NAME, StringAttributeType.INSTANCE);

  private Relation relation = ImmutableRelation.builder().relationId("r")
      .addAttribute(ATTR_NAME, true, StringAttributeType.INSTANCE)
      .build();

  @Test
  public void testApply() {
    IsNotNullOperator op = new IsNotNullOperator(ATTR_EXP);
    Tuple t = new TupleImpl(relation);
    assertFalse(op.apply(t));
    t.setAttribute(ATTR_NAME, "k");
    assertTrue(op.apply(t));
  }

}
