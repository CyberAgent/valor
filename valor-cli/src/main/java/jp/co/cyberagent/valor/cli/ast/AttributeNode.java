package jp.co.cyberagent.valor.cli.ast;

public class AttributeNode implements ExpressionNode {

  private String attribute;

  public AttributeNode(String attribute) {
    this.attribute = attribute;
  }

  public String getAttribute() {
    return attribute;
  }

  @Override
  public void accept(AstVisitor visitor) {
    visitor.visit(this);
    visitor.leave(this);
  }
}
