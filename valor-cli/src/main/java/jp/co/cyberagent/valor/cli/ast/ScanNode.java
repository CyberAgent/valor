package jp.co.cyberagent.valor.cli.ast;


public class ScanNode implements AstNode {

  private ProjectionClauseNode projection;
  private FromClauseNode from;
  private WhereClauseNode where;

  public ScanNode(ProjectionClauseNode projection, FromClauseNode from, WhereClauseNode where) {
    this.projection = projection;
    this.from = from;
    this.where = where;
  }


  @Override
  public void accept(AstVisitor visitor) {
    if (visitor.visit(this)) {
      projection.accept(visitor);
      from.accept(visitor);
      where.accept(visitor);
    }
    visitor.leave(this);
  }

  public ProjectionClauseNode getProjection() {
    return projection;
  }

  public FromClauseNode getFrom() {
    return from;
  }

  public WhereClauseNode getWhere() {
    return where;
  }
}
