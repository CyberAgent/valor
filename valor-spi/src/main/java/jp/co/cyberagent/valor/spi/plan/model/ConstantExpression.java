package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.FalseSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;

public class ConstantExpression<V> implements Expression<V> {

  public static final BooleanConstantExpression TRUE = new BooleanConstantExpression(true);
  public static final BooleanConstantExpression FALSE = new BooleanConstantExpression(false);

  public static class BooleanConstantExpression
      extends ConstantExpression<Boolean> implements PrimitivePredicate {

    public BooleanConstantExpression(Boolean value) {
      super(value);
    }

    @Override
    public FilterSegment buildFilterFragment() throws SerdeException {
      return value ? TrueSegment.INSTANCE : FalseSegment.INSTANCE;
    }

    @Override
    public PrimitivePredicate getNegation() {
      return value ? FALSE : TRUE;
    }

  }

  protected final V value;

  public ConstantExpression(V value) {
    this.value = value;
  }

  @Override
  public AttributeType getType() {
    return AttributeType.getTypeOf(value);
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    visitor.visit(this);
    visitor.leave(this);
  }

  @Override
  public V apply(Tuple tuple) {
    return value;
  }

  @Override
  public String toString() {
    return value == null ? "NULL" : value.toString();
  }

  public Object getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConstantExpression that = (ConstantExpression) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(value);
  }
}
