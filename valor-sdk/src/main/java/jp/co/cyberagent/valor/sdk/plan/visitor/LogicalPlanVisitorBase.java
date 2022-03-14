package jp.co.cyberagent.valor.sdk.plan.visitor;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.BinaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.FromClause;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;
import jp.co.cyberagent.valor.spi.plan.model.NotOperator;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionClause;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.plan.model.Query;
import jp.co.cyberagent.valor.spi.plan.model.UnaryPrimitivePredicate;
import jp.co.cyberagent.valor.spi.plan.model.WhereClause;

/**
 *
 */
public abstract class LogicalPlanVisitorBase<T> implements LogicalPlanVisitor {

  protected T result;

  protected Stack<List<T>> childResult = new Stack<List<T>>();

  public T walk(LogicalPlanNode node) {
    node.accept(this);
    return result;
  }

  public boolean visit(LogicalPlanNode node) {
    boolean returnValue = doVisit(node);
    this.childResult.push(new ArrayList<T>());
    return returnValue;
  }

  protected boolean doVisit(LogicalPlanNode e) {
    if (e instanceof Query) {
      return visitQuery((Query) e);
    }
    if (e instanceof ProjectionClause) {
      return visitProjectionClause((ProjectionClause) e);
    }
    if (e instanceof ProjectionItem) {
      return visitProjectionItem((ProjectionItem) e);
    }
    if (e instanceof FromClause) {
      return visitFromClause((FromClause) e);
    }
    if (e instanceof WhereClause) {
      return visitWhereClause((WhereClause) e);
    }
    if (e instanceof ConstantExpression) {
      return visitConstantExpression((ConstantExpression) e);
    }
    if (e instanceof AttributeNameExpression) {
      return visitAttributeNameExpression((AttributeNameExpression) e);
    }
    if (e instanceof BinaryPrimitivePredicate) {
      return visitBinaryPrimitivePredicate((BinaryPrimitivePredicate) e);
    }
    if (e instanceof UnaryPrimitivePredicate) {
      return visitUnaryPrimitivePredicate((UnaryPrimitivePredicate) e);
    }
    if (e instanceof AndOperator) {
      return visitAndOperator((AndOperator) e);
    }
    if (e instanceof OrOperator) {
      return visitOrOperator((OrOperator) e);
    }
    if (e instanceof NotOperator) {
      return visitNotOperator((NotOperator) e);
    }
    throw new IllegalStateException(e + " is not supported");
  }

  protected boolean visitQuery(Query e) {
    return true;
  }

  protected boolean visitProjectionClause(ProjectionClause e) {
    return true;
  }

  protected boolean visitProjectionItem(ProjectionItem e) {
    return true;
  }

  protected boolean visitFromClause(FromClause e) {
    return true;
  }

  protected boolean visitWhereClause(WhereClause e) {
    return true;
  }

  protected boolean visitConstantExpression(ConstantExpression e) {
    return true;
  }

  protected boolean visitAttributeNameExpression(AttributeNameExpression e) {
    return true;
  }

  protected boolean visitAndOperator(AndOperator e) {
    return true;
  }

  protected boolean visitOrOperator(OrOperator e) {
    return true;
  }

  protected boolean visitBinaryPrimitivePredicate(BinaryPrimitivePredicate e) {
    return true;
  }

  protected boolean visitUnaryPrimitivePredicate(UnaryPrimitivePredicate e) {
    return true;
  }

  protected boolean visitNotOperator(NotOperator e) {
    return true;
  }

  public void leave(LogicalPlanNode node) {
    doLeave(node);
    this.childResult.pop();
    if (!this.childResult.isEmpty()) {
      this.childResult.peek().add(this.result);
    }
  }

  protected void doLeave(LogicalPlanNode e) {
    if (e instanceof Query) {
      leaveQuery((Query) e);
    } else if (e instanceof ProjectionClause) {
      leaveProjectionClause((ProjectionClause) e);
    } else if (e instanceof ProjectionItem) {
      leaveProjectionItem((ProjectionItem) e);
    } else if (e instanceof FromClause) {
      leaveFromClause((FromClause) e);
    } else if (e instanceof WhereClause) {
      leaveWhereClause((WhereClause) e);
    } else if (e instanceof ConstantExpression) {
      leaveConstantExpression((ConstantExpression) e);
    } else if (e instanceof AttributeNameExpression) {
      leaveAttributeNameExpression((AttributeNameExpression) e);
    } else if (e instanceof BinaryPrimitivePredicate) {
      leaveBinaryPrimitivePredicate((BinaryPrimitivePredicate) e);
    } else if (e instanceof UnaryPrimitivePredicate) {
      leaveUnaryPrimitivePredicate((UnaryPrimitivePredicate) e);
    } else if (e instanceof AndOperator) {
      leaveAndOperator((AndOperator) e);
    } else if (e instanceof OrOperator) {
      leaveOrOperator((OrOperator) e);
    } else if (e instanceof NotOperator) {
      leaveNotOperator((NotOperator) e);
    } else {
      throw new IllegalStateException(e + " is not supported");
    }
  }

  protected void leaveQuery(Query e) {
  }

  protected void leaveProjectionClause(ProjectionClause e) {
  }

  protected void leaveProjectionItem(ProjectionItem e) {
  }

  protected void leaveFromClause(FromClause e) {
  }

  protected void leaveWhereClause(WhereClause e) {
  }

  protected void leaveConstantExpression(ConstantExpression e) {
  }

  protected void leaveAttributeNameExpression(AttributeNameExpression e) {
  }

  protected void leaveAndOperator(AndOperator e) {
  }

  protected void leaveOrOperator(OrOperator e) {
  }

  protected void leaveBinaryPrimitivePredicate(BinaryPrimitivePredicate e) {
  }

  protected void leaveUnaryPrimitivePredicate(UnaryPrimitivePredicate e) {
  }

  protected void leaveNotOperator(NotOperator e) {
  }


}
