package jp.co.cyberagent.valor.cli.ast;

public class RelationSourceNode implements FromClauseNode {

  private final String relationId;

  public RelationSourceNode(String relationId) {
    this.relationId = relationId;
  }

  public String getRelationId() {
    return relationId;
  }

  @Override
  public void accept(AstVisitor visitor) {
    visitor.visit(this);
    visitor.leave(this);
  }
}
