package jp.co.cyberagent.valor.sdk.serde.tree;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import jp.co.cyberagent.valor.sdk.plan.visitor.AttributeCollectVisitor;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.FalseSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeBasedQuerySerialzier implements QuerySerializer {

  static Logger LOG = LoggerFactory.getLogger(TreeBasedQuerySerialzier.class);

  private String currentField;
  private TreeNode<FilterSegment> currentParent;
  private List<TreeNode<FilterSegment>> nextParents;

  @Override
  public List<StorageScan> serailize(
      Collection<String> attributes, Collection<PrimitivePredicate> conjunction,
      List<FieldLayout> layouts) throws ValorException {
    attributes = new HashSet<>(attributes);
    attributes.addAll(new AttributeCollectVisitor().walk(conjunction));

    TreeNode<FilterSegment> root = ScanTreeNode.buildRootNode();
    Collection<TreeNode<FilterSegment>> parents = Arrays.asList(root);
    int maxRequiredFieldIndex = 0;
    for (int i = 0; i < layouts.size(); i++) {
      FieldLayout layout = layouts.get(i);
      currentField = layout.fieldName();
      for (Segment segment : layout.formatters()) {
        if (maxRequiredFieldIndex < i) {
          if (attributes.stream().anyMatch(segment::containsAttribute)) {
            maxRequiredFieldIndex = i;
          }
        }
        nextParents = new ArrayList<>();
        Collection<TreeNode<FilterSegment>> nextParents = new ArrayList<>();
        for (TreeNode<FilterSegment> parent : parents) {
          this.currentParent = parent;
          segment.accept(this, conjunction);
          nextParents.addAll(currentParent.getChildren());
        }
        parents = nextParents;
      }

    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(new TreeBaseSerde.ToStringVisitor<FilterSegment>().walk(root));
    }
    List<String> fields = IntStream.rangeClosed(0, maxRequiredFieldIndex)
        .mapToObj(layouts::get).map(FieldLayout::fieldName).collect(Collectors.toList());
    BuildScanVisitor visitor = new BuildScanVisitor(fields);
    return visitor.walk(root);
  }

  @Override
  public void write(String type, FilterSegment fragment) {
    this.currentParent.addChild(new ScanTreeNode(currentField, type, fragment));
  }

  static class BuildScanVisitor extends TreeBaseSerde.Visitor<List<StorageScan>, FilterSegment> {

    final List<String> fields;

    final Deque<TreeNode<FilterSegment>> path;

    public BuildScanVisitor(List<String> fields) {
      this.fields = fields;
      this.result = new ArrayList<>();
      this.path = new ArrayDeque<>();
    }

    @Override
    protected boolean doVisit(TreeNode<FilterSegment> node) {
      if (node.getParent() != null) { // ignore root node
        path.add(node);
      }
      if (node.getValue() instanceof FalseSegment) {
        return false;
      }
      if (node.getChildren().isEmpty()) {
        StorageScanImpl ss = new StorageScanImpl(fields);
        String field = null;
        List<FilterSegment> segments = null;
        for (TreeNode<FilterSegment> n : path) {
          if (!n.getFieldName().equals(field)) {
            if (segments != null) {
              ss.update(field, segments);
            }
            field = n.getFieldName();
            segments = new ArrayList<>();
          }
          segments.add(n.getValue());
        }
        this.result.add(ss);
      }
      return true;
    }

    @Override
    protected void doLeave(TreeNode<FilterSegment> node) {
      if (node.getParent() != null) {
        path.removeLast();
      }
    }
  }
}
