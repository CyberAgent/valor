package jp.co.cyberagent.valor.hive;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Stack;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;

public abstract class ExprNodeVisitor<R> {
  protected R result;

  protected Stack<List<R>> childResults = new Stack<List<R>>();

  public R walk(ExprNodeDesc node) {
    acceptedBy(node);
    return result;
  }

  public void acceptedBy(ExprNodeDesc node) {
    if (visit(node)) {
      List<ExprNodeDesc> children = node.getChildren();
      if (children != null) {
        for (ExprNodeDesc child : children) {
          acceptedBy(child);
        }
      }
    }
    leave(node);
  }

  public boolean visit(ExprNodeDesc node) {
    boolean proceed = doVisit(node);
    this.childResults.push(Lists.<R>newArrayList());
    return proceed;
  }

  public boolean doVisit(ExprNodeDesc node) {
    if (node instanceof ExprNodeColumnDesc) {
      return doVisitColumnDesc((ExprNodeColumnDesc) node);
    }
    if (node instanceof ExprNodeConstantDesc) {
      return doVisitConstantDesc((ExprNodeConstantDesc) node);
    }
    if (node instanceof ExprNodeFieldDesc) {
      return doVisitFieldDesc((ExprNodeFieldDesc) node);
    }
    if (node instanceof ExprNodeGenericFuncDesc) {
      return doVisitGenericFuncDesc((ExprNodeGenericFuncDesc) node);
    }
    throw new IllegalArgumentException(node + " is not supported");
  }

  protected boolean doVisitColumnDesc(ExprNodeColumnDesc node) {
    return false;
  }

  protected boolean doVisitConstantDesc(ExprNodeConstantDesc node) {
    return false;
  }

  protected boolean doVisitFieldDesc(ExprNodeFieldDesc node) {
    return false;
  }

  protected abstract boolean doVisitGenericFuncDesc(ExprNodeGenericFuncDesc node);

  public void leave(ExprNodeDesc node) {
    doLeave(node);
    this.childResults.pop();
    if (!this.childResults.isEmpty()) {
      this.childResults.peek().add(this.result);
    }
  }

  public void doLeave(ExprNodeDesc node) {
    if (node instanceof ExprNodeColumnDesc) {
      doLeaveColumnDesc((ExprNodeColumnDesc) node);
    } else if (node instanceof ExprNodeConstantDesc) {
      doLeaveConstantDesc((ExprNodeConstantDesc) node);
    } else if (node instanceof ExprNodeFieldDesc) {
      doLeaveFieldDesc((ExprNodeFieldDesc) node);
    } else if (node instanceof ExprNodeGenericFuncDesc) {
      doLeaveGenericFuncDesc((ExprNodeGenericFuncDesc) node);
    } else {
      throw new IllegalArgumentException(node + " is not supported");
    }
  }

  protected void doLeaveColumnDesc(ExprNodeColumnDesc node) {
  }

  protected void doLeaveConstantDesc(ExprNodeConstantDesc node) {
  }

  protected void doLeaveFieldDesc(ExprNodeFieldDesc node) {
  }

  protected abstract void doLeaveGenericFuncDesc(ExprNodeGenericFuncDesc node);
}
