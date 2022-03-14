package jp.co.cyberagent.valor.cli.ast;

public interface AstNode {

  void accept(AstVisitor visitor);

}
