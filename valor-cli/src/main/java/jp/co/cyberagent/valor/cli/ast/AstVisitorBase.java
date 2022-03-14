package jp.co.cyberagent.valor.cli.ast;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public abstract class AstVisitorBase<T> implements AstVisitor {

  protected T result;

  protected Stack<List<T>> childResult = new Stack<List<T>>();

  public T walk(AstNode node) {
    node.accept(this);
    return result;
  }

  public boolean visit(AstNode node) {
    boolean returnValue = doVisit(node);
    this.childResult.push(new ArrayList<T>());
    return returnValue;
  }

  private boolean doVisit(AstNode node) {
    if (node instanceof ScanNode) {
      return visitQueryNode((ScanNode)node);
    } else if (node instanceof ProjectionClauseNode) {
      return visitProjectionClauseNode((ProjectionClauseNode)node);
    } else if (node instanceof FromClauseNode) {
      return visitFromClauseNode((FromClauseNode)node);
    } else if (node instanceof WhereClauseNode) {
      return visitWhereClauseNode((WhereClauseNode)node);
    } else if (node instanceof ExpressionNode) {
      return visitExpressionNode((ExpressionNode)node);
    }
    throw new IllegalArgumentException("unsupported node " + node);
  }

  protected boolean visitQueryNode(ScanNode node) {
    return true;
  }

  protected boolean visitProjectionClauseNode(ProjectionClauseNode node) {
    return true;
  }

  protected boolean visitFromClauseNode(FromClauseNode node) {
    if (node instanceof RelationSourceNode) {
      return visitRelationSourceNode((RelationSourceNode)node);
    } else {
      throw new IllegalArgumentException("unsupported source " + node);
    }
  }

  protected boolean visitRelationSourceNode(RelationSourceNode node) {
    return true;
  }

  protected boolean visitWhereClauseNode(WhereClauseNode node) {
    return true;
  }

  protected boolean visitExpressionNode(ExpressionNode node) {
    if (node instanceof AttributeNode) {
      return visitAttributeNode((AttributeNode)node);
    } else if (node instanceof ConstantNode) {
      return visitConstantNode((ConstantNode)node);
    } else if (node instanceof FunctionNode) {
      return visitFunctionNode((FunctionNode)node);
    } else if (node instanceof AllItemNode) {
      return visitAllItemNode((AllItemNode)node);
    }
    throw new IllegalArgumentException("unsupported expression node " + node);
  }

  protected boolean visitAttributeNode(AttributeNode node) {
    return true;
  }

  protected boolean visitConstantNode(ConstantNode node) {
    return true;
  }

  protected boolean visitFunctionNode(FunctionNode node) {
    return true;
  }

  protected boolean visitAllItemNode(AllItemNode node) {
    return true;
  }

  public void leave(AstNode node) {
    doLeave(node);
    this.childResult.pop();
    if (!this.childResult.isEmpty()) {
      this.childResult.peek().add(this.result);
    }
  }

  protected void doLeave(AstNode node) {
    if (node instanceof ScanNode) {
      leaveQueryNode((ScanNode)node);
    } else if (node instanceof ProjectionClauseNode) {
      leaveProjectionClauseNode((ProjectionClauseNode)node);
    } else if (node instanceof FromClauseNode) {
      leaveFromClauseNode((FromClauseNode)node);
    } else if (node instanceof WhereClauseNode) {
      leaveWhereClauseNode((WhereClauseNode)node);
    } else if (node instanceof ExpressionNode) {
      leaveExpressionNode((ExpressionNode) node);
    } else {
      throw new IllegalArgumentException("unsupported node " + node);
    }
  }

  protected void leaveQueryNode(ScanNode node) {
  }

  protected void leaveProjectionClauseNode(ProjectionClauseNode node) {
  }

  protected void leaveFromClauseNode(FromClauseNode node) {
    if (node instanceof RelationSourceNode) {
      leaveRelationSourceNode((RelationSourceNode)node);
    } else {
      throw new IllegalArgumentException("unsupported source " + node);
    }
  }

  protected void leaveRelationSourceNode(RelationSourceNode node) {
  }

  protected void leaveWhereClauseNode(WhereClauseNode node) {
  }

  protected void leaveExpressionNode(ExpressionNode node) {
    if (node instanceof AttributeNode) {
      leaveAttributeNode((AttributeNode)node);
    } else if (node instanceof ConstantNode) {
      leaveConstantNode((ConstantNode)node);
    } else if (node instanceof FunctionNode) {
      leaveFunctionNode((FunctionNode)node);
    } else if (node instanceof AllItemNode) {
      leaveAllItemNode((AllItemNode)node);
    } else {
      throw new IllegalArgumentException("unsupported expression node " + node);
    }
  }

  protected void leaveAttributeNode(AttributeNode node) {
  }

  protected void leaveConstantNode(ConstantNode node) {
  }

  protected void leaveFunctionNode(FunctionNode node) {
  }

  protected void leaveAllItemNode(AllItemNode node) {
  }

}
