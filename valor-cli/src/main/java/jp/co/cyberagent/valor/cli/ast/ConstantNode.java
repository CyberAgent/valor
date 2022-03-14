package jp.co.cyberagent.valor.cli.ast;

public class ConstantNode implements ExpressionNode {

  private final Object value;

  public ConstantNode(Object value) {
    this.value = value;
  }

  public Object getValue() {
    return value;
  }

  @Override
  public void accept(AstVisitor visitor) {
    visitor.visit(this);
    visitor.leave(this);
  }
}
