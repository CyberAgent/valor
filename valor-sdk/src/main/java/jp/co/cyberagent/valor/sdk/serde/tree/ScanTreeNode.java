package jp.co.cyberagent.valor.sdk.serde.tree;

import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;

public class ScanTreeNode extends TreeNodeBase<FilterSegment> {

  public ScanTreeNode(String fieldName, String type, FilterSegment value) {
    super(fieldName, type, value);
  }

  public static ScanTreeNode buildRootNode() {
    return new ScanTreeNode(null, DEFAULT_DESCRIPTOR,
        new CompleteMatchSegment(null, new byte[0]));
  }

  @Override
  public String toString() {
    return String.format("%s %s", fieldName, value);
  }
}

