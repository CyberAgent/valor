package jp.co.cyberagent.valor.cli.ast;

import java.util.Arrays;
import java.util.List;

public class ProjectionClauseNode implements AstNode {

  private List<ExpressionNode> items;

  public ProjectionClauseNode(ExpressionNode... items) {
    this(Arrays.asList(items));
  }

  public ProjectionClauseNode(List<ExpressionNode> items) {
    this.items = items;
  }

  @Override
  public void accept(AstVisitor visitor) {
    if (visitor.visit(this)) {
      for (ExpressionNode item: items) {
        item.accept(visitor);
      }
    }
    visitor.leave(this);
  }
}
