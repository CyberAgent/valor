package jp.co.cyberagent.valor.spi.plan.model;

import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

public class UdfExpression implements Expression {

  protected final Udf udf;

  protected final List<Expression> argExps;

  public UdfExpression(Udf udf, List<Expression> args) {
    this.udf = udf;
    this.argExps = args;
    this.udf.init(argExps);
  }

  public Udf getFunction() {
    return udf;
  }

  public List<Expression> getArguments() {
    return argExps;
  }

  @Override
  public AttributeType getType() {
    return udf.getReturnType();
  }

  @Override
  public Object apply(Object t) {
    Object[] args = new Object[argExps.size()];
    for (int i = 0; i < args.length; i++) {
      args[i] = argExps.get(i).apply(t);
    }
    return udf.apply(args);
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      argExps.forEach(e -> e.accept(visitor));
    }
    visitor.leave(this);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UdfExpression that = (UdfExpression) o;
    return Objects.equals(udf, that.udf) && Objects.equals(argExps, that.argExps);
  }

  @Override
  public int hashCode() {
    return Objects.hash(udf, argExps);
  }
}
