package jp.co.cyberagent.valor.spi.plan.model;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.BooleanAttributeType;
import jp.co.cyberagent.valor.spi.util.HeteroDigitCounter;

public interface PredicativeExpression extends Expression<Boolean>, Predicate<Tuple> {

  static OrOperator mergeByAnd(OrOperator dnf1, OrOperator dnf2) {
    assertNormalized(dnf1);
    assertNormalized(dnf2);
    OrOperator mergedDnf = new OrOperator();
    for (PredicativeExpression lc : dnf1) {
      List<PredicativeExpression> lo = ((AndOperator)lc).getOperands();
      for (PredicativeExpression rc : dnf2) {
        List<PredicativeExpression> ro = ((AndOperator)rc).getOperands();
        AndOperator mc = new AndOperator();
        lo.forEach(mc::addOperand);
        ro.forEach(mc::addOperand);
        mergedDnf.addOperand(mc);
      }
    }
    return mergedDnf;
  }

  static OrOperator mergeByOr(OrOperator dnf1, OrOperator dnf2) {
    assertNormalized(dnf1);
    assertNormalized(dnf2);
    OrOperator merged = new OrOperator();
    dnf1.getOperands().forEach(merged::addOperand);
    dnf2.getOperands().forEach(merged::addOperand);
    return merged;
  }

  static OrOperator getNegation(OrOperator dnf) {
    assertNormalized(dnf);
    OrOperator negation = new OrOperator();
    List<List<PredicativeExpression>> disjuncts = dnf.getOperands().stream()
        .map(AndOperator.class::cast)
        .map(AndOperator::getOperands)
        .collect(Collectors.toList());
    HeteroDigitCounter counter = HeteroDigitCounter.buildHeteroDigitCounter(disjuncts);
    while (counter.next() != null) {
      int[] c = counter.getCounter();
      AndOperator conjunction = new AndOperator();
      for (int i = 0; i < c.length; i++) {
        PrimitivePredicate orgCond = (PrimitivePredicate) disjuncts.get(i).get(c[i]);
        conjunction.addOperand(orgCond.getNegation());
      }
      negation.addOperand(conjunction);
    }
    return negation;
  }

  static void assertNormalized(OrOperator predicate) {
    for (PredicativeExpression lc : predicate) {
      if (!(lc instanceof AndOperator)) {
        throw new IllegalArgumentException(predicate + " is not normalized in DNF");
      }
      AndOperator conjunction = (AndOperator) lc;
      for (PredicativeExpression p : conjunction) {
        if (!(p instanceof PrimitivePredicate)) {
          throw new IllegalArgumentException(predicate + " is not normalized in DNF");
        }
      }
    }
  }

  OrOperator getDnf();

  @Override
  default AttributeType getType() {
    return BooleanAttributeType.INSTANCE;
  }

  @Override
  default boolean test(Tuple tuple) {
    Object v = apply(tuple);
    if (v == null) {
      return false;
    }
    if (v instanceof Boolean) {
      return ((Boolean) v).booleanValue();
    }
    throw new IllegalStateException(this + " is not boolean expression");
  }

}
