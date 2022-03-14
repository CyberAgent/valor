package jp.co.cyberagent.valor.cli.ast;

public interface AstVisitor {

  boolean visit(AstNode node);

  void leave(AstNode node);

}
