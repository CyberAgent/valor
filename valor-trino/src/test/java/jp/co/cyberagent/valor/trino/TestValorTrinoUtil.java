package jp.co.cyberagent.valor.trino;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.IntegerType;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.util.Pair;
import org.junit.jupiter.api.Test;

public class TestValorTrinoUtil {

  @Test
  public void testToExpression() {
    Relation relation = ImmutableRelation.builder().relationId("r")
        .addAttribute("attr", true, IntegerAttributeType.INSTANCE).build();
    Range range = Range.range(IntegerType.INTEGER, 100l, true, 100l, true);
    ValueSet vset = ValueSet.ofRanges(range);
    Map<ColumnHandle, Domain> domain = new HashMap<ColumnHandle, Domain>() {{
      put(new ValorColumnHandle(null, "attr", IntegerType.INTEGER), Domain.create(vset, false));
    }};
    TupleDomain<ColumnHandle> constraint = TupleDomain.withColumnDomains(domain);
    Expression exp = ValorTrinoUtil.toPredicativeExpression(relation, constraint);
    assertThat(exp, is(instanceOf(EqualOperator.class)));
    Pair<String, Object> eq = ((EqualOperator) exp).getAttributeAndConstantIfExists();
    assertThat(eq.getFirst(), equalTo("attr"));
    assertThat(eq.getSecond(), equalTo(100));

  }
}
