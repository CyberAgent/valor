package jp.co.cyberagent.valor.sdk.serde.tree;

public class RecordTreeNode extends TreeNodeBase<byte[]> {

  public RecordTreeNode(String fieldName, String type, byte[] value) {
    super(fieldName, type, value);
  }

  public static RecordTreeNode buildRootNode() {
    return new RecordTreeNode(null, DEFAULT_DESCRIPTOR, new byte[0]);
  }

  @Override
  public String toString() {
    return String.format("%s = %s", fieldName, value);
  }
}
