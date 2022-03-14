package jp.co.cyberagent.valor.spi.plan.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.Tuple;

public class AndOperator implements PredicativeExpression, Iterable<PredicativeExpression> {

  private List<PredicativeExpression> operands;

  public static PredicativeExpression join(PredicativeExpression... args) {
    if (args.length == 0) {
      return ConstantExpression.TRUE;
    }
    if (args.length == 1) {
      return args[0];
    }
    return new AndOperator(Arrays.asList(args));
  }

  public static PredicativeExpression join(Collection<PredicativeExpression> args) {
    return join(args.toArray(new PredicativeExpression[args.size()]));
  }

  public AndOperator() {
    this.operands = new ArrayList<>();
  }

  private AndOperator(List<PredicativeExpression> args) {
    this.operands = new ArrayList<>(args);
  }

  @Override
  public OrOperator getDnf() {
    OrOperator dnf = operands.get(0).getDnf();
    if (operands.size() == 1) {
      return dnf;
    }
    for (int i = 1; i < operands.size(); i++) {
      dnf = PredicativeExpression.mergeByAnd(dnf, operands.get(i).getDnf());
    }
    return dnf;
  }

  @Override
  public Boolean apply(Tuple tuple) {
    return operands.stream().allMatch(o -> o.test(tuple));
  }

  @Override
  public Iterator<PredicativeExpression> iterator() {
    return operands.iterator();
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      operands.forEach(o -> o.accept(visitor));
    }
    visitor.leave(this);
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(" AND ");
    for (PredicativeExpression o : this.operands) {
      joiner.add(o.toString());
    }
    return joiner.toString();
  }

  public List<PredicativeExpression> getOperands() {
    return operands;
  }

  public void addOperand(PredicativeExpression p) {
    this.operands.add(p);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AndOperator)) {
      return false;
    }
    AndOperator that = (AndOperator) o;
    return Objects.equals(operands, that.operands);
  }

  @Override
  public int hashCode() {
    return Objects.hash(operands);
  }
}
