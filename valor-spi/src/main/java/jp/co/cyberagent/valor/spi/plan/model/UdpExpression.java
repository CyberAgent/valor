package jp.co.cyberagent.valor.spi.plan.model;

import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;

public class UdpExpression implements PrimitivePredicate {

  protected final Udf<Boolean> udf;

  protected final List<Expression> argExps;

  private final boolean negate;

  public UdpExpression(Udf<Boolean> udf, List<Expression> args) {
    this(udf, args, false);
  }

  public UdpExpression(Udf<Boolean> udf, List<Expression> args, boolean negate) {
    this.udf = udf;
    this.argExps = args;
    this.negate = negate;
  }

  @Override
  public AttributeType getType() {
    return udf.getReturnType();
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      argExps.forEach(e -> e.accept(visitor));
    }
    visitor.leave(this);
  }

  @Override
  public FilterSegment buildFilterFragment() throws SerdeException {
    return TrueSegment.INSTANCE;
  }

  @Override
  public PrimitivePredicate getNegation() {
    return new UdpExpression(udf, argExps, !negate);
  }

  @Override
  public Boolean apply(Tuple t) {
    Object[] args = new Object[argExps.size()];
    for (int i = 0; i < args.length; i++) {
      args[i] = argExps.get(i).apply(t);
    }
    return udf.apply(args);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UdpExpression that = (UdpExpression) o;
    return negate == that.negate && Objects.equals(udf, that.udf)
        && Objects.equals(argExps, that.argExps);
  }

  @Override
  public int hashCode() {
    return Objects.hash(udf, argExps, negate);
  }
}
