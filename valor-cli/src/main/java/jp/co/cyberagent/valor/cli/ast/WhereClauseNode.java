package jp.co.cyberagent.valor.cli.ast;

public class WhereClauseNode implements AstNode {

  private final ExpressionNode predicate;

  public WhereClauseNode(ExpressionNode predicate) {
    this.predicate = predicate;
  }

  public ExpressionNode getPredicate() {
    return predicate;
  }

  @Override
  public void accept(AstVisitor visitor) {
    if (visitor.visit(this)) {
      predicate.accept(visitor);
    }
    visitor.leave(this);
  }
}
