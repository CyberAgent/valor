package jp.co.cyberagent.valor.sdk.serde.tree;

import java.util.Base64;
import java.util.List;
import jp.co.cyberagent.valor.spi.exception.ValorException;

public interface TreeNode<V> {

  String DEFAULT_DESCRIPTOR = "";
  String ATTRIBUTE_NAME_TYPE_PREFIX = "attrName_";
  String MAP_KEY_TYPE_PREFIX_FORMAT = "mapKey_%s_";

  Base64.Encoder encoder = Base64.getEncoder();
  Base64.Decoder decoder = Base64.getDecoder();

  static String attrType(String attr) {
    return ATTRIBUTE_NAME_TYPE_PREFIX + attr;
  }

  static String mapKeyDesc(String mapAttr, byte[] key) {
    return String.format(MAP_KEY_TYPE_PREFIX_FORMAT, mapAttr) + encoder.encodeToString(key);
  }

  static byte[] parseMapKeyType(String mapAttr, String mapKeyTypeDesc) {
    String prefix = String.format(MAP_KEY_TYPE_PREFIX_FORMAT, mapAttr);
    if (mapKeyTypeDesc.startsWith(prefix)) {
      String encodedKey = mapKeyTypeDesc.substring(prefix.length());
      return decoder.decode(encodedKey);
    }
    return null;
  }

  String getFieldName();

  void accept(TreeBaseSerde.Visitor<?, V> visitor) throws ValorException;

  TreeNode<V> getParent();

  void setParent(TreeNode<V> parent);

  void addChild(TreeNode<V> child);

  List<TreeNode<V>> getChildren();

  String getType();

  V getValue();

  void setValue(V val);
}
