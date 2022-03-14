package jp.co.cyberagent.valor.sdk.plan.function;

import java.util.List;
import java.util.function.Predicate;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;

@Deprecated
public class ArrayContainsFunction implements PrimitivePredicate {

  private Predicate<Tuple> predicate;

  public ArrayContainsFunction(String attr, Object val) {
    this((t) -> {
      List<Object> array = (List<Object>) t.getAttribute(attr);
      return array.contains(val);
    });
  }

  public ArrayContainsFunction(Predicate<Tuple> predicate) {
    this.predicate = predicate;
  }


  @Override
  public FilterSegment buildFilterFragment() throws SerdeException {
    return TrueSegment.INSTANCE;
  }

  @Override
  public PrimitivePredicate getNegation() {
    return new ArrayContainsFunction(predicate.negate());
  }

  @Override
  public Boolean apply(Tuple tuple) {
    return predicate.test(tuple);
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    visitor.visit(this);
    visitor.leave(this);
  }
}
