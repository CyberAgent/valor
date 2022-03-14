package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Objects;
import jp.co.cyberagent.valor.spi.exception.NonConstantException;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AttributeNameExpression implements Expression<Object> {
  static final Logger LOG = LoggerFactory.getLogger(AttributeNameExpression.class);

  private String name;

  private AttributeType type;

  /**
   * @param name attribute name
   * @param type attribute type
   */
  public AttributeNameExpression(String name, AttributeType type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return this.name;
  }

  @Override
  public AttributeType getType() {
    return type;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    visitor.visit(this);
    visitor.leave(this);
  }

  @Override
  public Object apply(Tuple tuple) {
    if (tuple == null) {
      throw new NonConstantException("attribute " + name + " is not constant");
    }
    return tuple.getAttribute(name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof AttributeNameExpression)) {
      return false;
    }
    AttributeNameExpression that = (AttributeNameExpression) o;
    return Objects.equals(name, that.name) && Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }
}
