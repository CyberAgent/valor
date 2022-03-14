package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Objects;
import java.util.Optional;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.util.Pair;

public abstract class BinaryPrimitivePredicate implements PrimitivePredicate {

  protected final Expression right;

  protected final Expression left;

  public BinaryPrimitivePredicate(Expression left, Expression right) {
    this.left = left;
    this.right = right;
  }

  public BinaryPrimitivePredicate(String attr, AttributeType type, Object value) {
    this(new AttributeNameExpression(attr, type), new ConstantExpression(value));
  }

  public Expression getRight() {
    return right;
  }

  public Expression getLeft() {
    return left;
  }

  @Override
  public String toString() {
    return String.format("%s %s %s", left == null ? "null" : left.toString(),
        getOperatorExpression(), right == null ? "null" : right.toString());
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    if (visitor.visit(this)) {
      left.accept(visitor);
      right.accept(visitor);
    }
    visitor.leave(this);
  }


  @SuppressWarnings("unchecked")
  @Override
  public Boolean apply(Tuple tuple) {
    Object lv = left.apply(tuple);
    Object rv = right.apply(tuple);
    return test(lv, rv);
  }

  protected abstract String getOperatorExpression();

  protected abstract boolean test(Object lv, Object rv);

  @Override
  public FilterSegment buildFilterFragment() throws SerdeException {
    if (left instanceof AttributeNameExpression) {
      if (right instanceof ConstantExpression) {
        return buildAttributeCompareFragment((AttributeNameExpression) left,
            (ConstantExpression) right, false);
      }
    } else if (right instanceof AttributeNameExpression) {
      if (left instanceof ConstantExpression) {
        return buildAttributeCompareFragment((AttributeNameExpression) right,
            (ConstantExpression) left, true);
      }
    }
    return TrueSegment.INSTANCE;
  }

  protected abstract FilterSegment buildAttributeCompareFragment(AttributeNameExpression attr,
                                                                 ConstantExpression value,
                                                                 boolean swapped)
      throws SerdeException;

  @Deprecated
  public String getAttributeIfUnaryPredicate() {
    if (left instanceof AttributeNameExpression) {
      if (right instanceof ConstantExpression) {
        return ((AttributeNameExpression) left).getName();
      }
    } else if (right instanceof AttributeNameExpression) {
      if (left instanceof ConstantExpression) {
        return ((AttributeNameExpression) right).getName();
      }
    }
    return null;
  }

  public Optional<AttributeNameExpression> getAttributeExpIfUnaryPredicate() {
    if (left instanceof AttributeNameExpression) {
      if (right instanceof ConstantExpression) {
        return Optional.of((AttributeNameExpression) left);
      }
    } else if (right instanceof AttributeNameExpression) {
      if (left instanceof ConstantExpression) {
        return Optional.of((AttributeNameExpression) right);
      }
    }
    return Optional.empty();
  }

  public Pair<String, Object> getAttributeAndConstantIfExists() {
    if (left instanceof AttributeNameExpression) {
      if (right instanceof ConstantExpression) {
        return new Pair<>(((AttributeNameExpression) left).getName(),
            ((ConstantExpression) right).getValue());
      }
    } else if (right instanceof AttributeNameExpression) {
      if (left instanceof ConstantExpression) {
        return new Pair<>(((AttributeNameExpression) right).getName(),
            ((ConstantExpression) left).getValue());
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BinaryPrimitivePredicate that = (BinaryPrimitivePredicate) o;
    return Objects.equals(right, that.right) && Objects.equals(left, that.left);
  }

  @Override
  public int hashCode() {
    return Objects.hash(right, left);
  }
}
