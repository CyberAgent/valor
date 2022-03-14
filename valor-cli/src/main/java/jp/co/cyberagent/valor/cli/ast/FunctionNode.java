package jp.co.cyberagent.valor.cli.ast;

import java.util.Arrays;
import java.util.List;

public class FunctionNode implements ExpressionNode {

  private final String name;
  private final List<ExpressionNode> arguments;

  public FunctionNode(String name, ExpressionNode... arguments) {
    this(name, Arrays.asList(arguments));
  }

  public FunctionNode(String name, List<ExpressionNode> arguments) {
    this.name = name;
    this.arguments = arguments;
  }

  public String getName() {
    return name;
  }

  public List<ExpressionNode> getArguments() {
    return arguments;
  }

  @Override
  public void accept(AstVisitor visitor) {
    if (visitor.visit(this)) {
      for (ExpressionNode arg : arguments) {
        arg.accept(visitor);
      }
    }
    visitor.leave(this);
  }
}
