package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import jp.co.cyberagent.valor.sdk.serde.ContinuousRecordsDeserializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeNode;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

public class MultiAttributeNamesFormatter extends MultiAttributesFormatter {

  public static final String FORMATTER_TYPE = "attrName";

  public static final byte[] EMPTY_MARKER = new byte[] {0x00};

  public static MultiAttributeNamesFormatter create(String... excludes) {
    MultiAttributeNamesFormatter e = new MultiAttributeNamesFormatter();
    e.setExcludedAttributes(Arrays.asList(excludes));
    return e;
  }

  @Override
  public Order getOrder() {
    return Order.NORMAL;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, final TupleDeserializer target)
      throws SerdeException {
    target.setState(ContinuousRecordsDeserializer.ATTR_NAME_STATE_KEY, in, offset, length);
    return length;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) {
    if (!(serializer instanceof TreeBasedTupleSerializer)) {
      throw new IllegalArgumentException();
    }
    Set<String> targetAttributes = this.getTargetAttributes(tuple);
    if (targetAttributes.isEmpty()) {
      // set empty string for tuples which attributes are completed in rowkey.
      // TODO change marker null character (0x00)
      serializer.write(TreeNode.ATTRIBUTE_NAME_TYPE_PREFIX, EMPTY_MARKER);
      return;
    }
    for (String key : targetAttributes) {
      if (tuple.getAttribute(key) == null) {
        continue;
      }
      // TODO improve readability
      serializer.write(TreeNode.ATTRIBUTE_NAME_TYPE_PREFIX, ByteUtils.toBytes(key));
    }
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction) {
    // FIXME create filtering fragment
    serializer.write(TreeNode.ATTRIBUTE_NAME_TYPE_PREFIX, TrueSegment.INSTANCE);
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  public String toString() {
    return String.format("%s[]", FORMATTER_TYPE);
  }



  public static class Factory implements FormatterFactory {

    @Override
    public Formatter create(Map config) {
      Object excludes = config.get(EXCLUDED_ATTRS_PROPKEY);
      return create(excludes == null ? Collections.emptyList() : (List<String>) excludes);
    }

    public MultiAttributeNamesFormatter create(List<String> excludes) {
      MultiAttributeNamesFormatter e = new MultiAttributeNamesFormatter();
      e.setExcludedAttributes(excludes);
      return e;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return MultiAttributeNamesFormatter.class;
    }
  }
}
