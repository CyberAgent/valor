package jp.co.cyberagent.valor.sdk.serde.tree;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.storage.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TreeBasedTupleSerializer implements TupleSerializer {

  static Logger LOG = LoggerFactory.getLogger(TreeBasedTupleSerializer.class);

  private String currentField;
  private TreeNode<byte[]> currentParent;
  private List<TreeNode<byte[]>> nextParents;

  @Override
  public List<Record> serailize(Tuple tuple, List<FieldLayout> layouts) throws ValorException {
    RecordTreeNode root = RecordTreeNode.buildRootNode();
    List<TreeNode<byte[]>> parents = Arrays.asList(root);
    for (FieldLayout layout : layouts) {
      currentField = layout.getFieldName();
      for (Segment f : layout.getFormatters()) {
        nextParents = new ArrayList<>();
        for (TreeNode<byte[]> parent : parents) {
          this.currentParent = parent;
          f.accept(this, tuple);
          nextParents.addAll(currentParent.getChildren());
        }
        parents = nextParents;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(new TreeBaseSerde.ToStringVisitor<byte[]>().walk(root));
    }
    BuildRecordVisitor visitor = new BuildRecordVisitor();
    return visitor.walk(root);
  }

  @Override
  public void write(String type, byte[]... values) {
    byte[] value;
    if (values.length == 1) {
      value = values[0];
    } else {
      ByteBuffer buf = ByteBuffer.allocate(Arrays.stream(values).mapToInt(v -> v.length).sum());
      for (int i = 0; i < values.length; i++) {
        buf.put(values[i]);
      }
      value = buf.array();
    }
    this.currentParent.addChild(new RecordTreeNode(currentField, type, value));
  }

  public TreeNode<byte[]> getCurrentParent() {
    return currentParent;
  }

  public static class BuildRecordVisitor extends TreeBaseSerde.Visitor<List<Record>, byte[]> {

    public BuildRecordVisitor() {
      this.result = new ArrayList();
    }

    @Override
    protected void preVisit(TreeNode<byte[]> node) throws ValorException {
      if (node.getValue() == null) {
        throw new SerdeException("visit node with null value");
      }
    }

    @Override
    protected void doLeave(TreeNode<byte[]> node) throws ValorException {
      if (node.getChildren().isEmpty()) {
        Record cell = new Record.RecordImpl();
        for (Map.Entry<String, List<byte[]>> e : values.entrySet()) {
          cell.setBytes(e.getKey(), join(e.getValue()));
        }
        result.add(cell);
      }
    }

    private byte[] join(List<byte[]> values) {
      ByteBuffer buf = ByteBuffer.allocate(values.stream().mapToInt(v -> v.length).sum());
      for (byte[] v : values) {
        buf.put(v);
      }
      return buf.array();
    }
  }
}
