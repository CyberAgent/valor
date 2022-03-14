package jp.co.cyberagent.valor.cli.ast;

public class AllItemNode implements ExpressionNode {

  @Override
  public void accept(AstVisitor visitor) {
    visitor.visit(this);
    visitor.leave(this);
  }
}
