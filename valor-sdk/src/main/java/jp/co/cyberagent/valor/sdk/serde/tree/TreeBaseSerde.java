package jp.co.cyberagent.valor.sdk.serde.tree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TreeBaseSerde {

  protected abstract static class Visitor<R, V> {

    static final Logger LOG = LoggerFactory.getLogger(Visitor.class);

    protected TreeNode<V> prevNode;

    protected Map<String, List<V>> values;

    protected R result;

    protected boolean resultInitiated;

    public R walk(TreeNode<V> root) throws ValorException {
      values = new HashMap<>();
      root.accept(this);
      return this.getResult();
    }

    public boolean visit(TreeNode<V> node) throws ValorException {
      boolean returnValue;
      preVisit(node);
      if (node.getParent() == null) {
        returnValue = doVisit(node);
      } else {
        returnValue = doVisit(node);
        List<V> value = values.get(node.getFieldName());
        if (value == null) {
          value = new ArrayList<>();
          values.put(node.getFieldName(), value);
        }
        value.add(node.getValue());
      }
      postVisit(node);
      prevNode = node;
      return returnValue;
    }

    protected boolean doVisit(TreeNode<V> node) throws ValorException {
      return true;
    }

    protected void preVisit(TreeNode<V> node) throws ValorException {
      if (LOG.isDebugEnabled()) {
        LOG.debug("start visit " + node);
      }
    }

    protected void postVisit(TreeNode<V> node) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("end visit   " + node);
      }
    }

    public void leave(TreeNode<V> node) throws ValorException {
      preLeave(node);
      doLeave(node);
      postLeave(node);
      List<V> value = values.get(node.getFieldName());
      if (node.getParent() != null) {
        value.remove(value.size() - 1);
      }
      prevNode = node;
    }

    protected void doLeave(TreeNode<V> node) throws ValorException {
    }

    protected void preLeave(TreeNode<V> node) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("start leave " + node);
      }
    }

    protected void postLeave(TreeNode<V> node) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("end leave   " + node);
      }
    }

    public R getResult() {
      if (!this.resultInitiated) {
        initResult();
      }
      return result;
    }

    protected void initResult() {
    }

  }

  protected static class ToStringVisitor<V> extends Visitor<String, V> {

    private StringBuilder buf = new StringBuilder();
    private int indent = 0;

    @Override
    protected boolean doVisit(TreeNode<V> node) {
      appendNode(node);
      indent++;
      return true;
    }

    private void appendNode(TreeNode<V> node) {
      for (int i = 0; i < indent; i++) {
        buf.append(" ");
      }
      buf.append(node.toString());
      buf.append(System.getProperty("line.separator"));
    }

    @Override
    public void leave(TreeNode<V> node) {
      indent--;
    }

    @Override
    public String getResult() {
      return buf.toString();
    }
  }

}
