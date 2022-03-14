package jp.co.cyberagent.valor.sdk.serde.tree;

import java.util.ArrayList;
import java.util.List;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TreeNodeBase<V> implements TreeNode<V> {

  public static final Logger LOG = LoggerFactory.getLogger(TreeNodeBase.class);
  protected final String type;
  protected String fieldName;
  protected List<TreeNode<V>> children;
  protected V value;
  private TreeNode<V> parent;

  public TreeNodeBase(String fieldName, String type, V value) {
    this.fieldName = fieldName;
    this.type = type;
    this.value = value;
    this.children = new ArrayList<>();
  }

  @Override
  public TreeNode<V> getParent() {
    return parent;
  }

  @Override
  public void setParent(TreeNode<V> parent) {
    this.parent = parent;
  }

  @Override
  public void addChild(TreeNode<V> child) {
    this.children.add(child);
    child.setParent(this);
  }

  @Override
  public List<TreeNode<V>> getChildren() {
    return children;
  }

  @Override
  public void accept(TreeBaseSerde.Visitor<?, V> visitor) throws ValorException {
    if (visitor.visit(this)) {
      for (TreeNode<V> node : children) {
        node.accept(visitor);
      }
    }
    visitor.leave(this);
  }

  @Override
  public String getType() {
    return type;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public void setValue(V val) {
    this.value = val;
  }

  @Override
  public String getFieldName() {
    return fieldName;
  }
}
