package jp.co.cyberagent.valor.sdk.formatter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.serde.ContinuousRecordsDeserializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeNode;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

/** 
 * must follow {@link MultiAttributeNamesFormatter}. 
 */
public class MultiAttributeValuesFormatter extends MultiAttributesFormatter {

  public static final String FORMATTER_TYPE = "attrValue";
  public static final String CUSTOM_FORMATTER_ATTRS_PROPKEY = "custom";
  private Map<String, Formatter> attrNameToCustomFormatter = Collections.emptyMap();

  public static MultiAttributeValuesFormatter create(String... excludes) {
    MultiAttributeValuesFormatter e = new MultiAttributeValuesFormatter();
    e.setExcludedAttributes(Arrays.asList(excludes));
    return e;
  }

  @Override
  public Order getOrder() {
    // TODO check attrNameToCustomFormatter
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    byte[] attrName = target.getState(ContinuousRecordsDeserializer.ATTR_NAME_STATE_KEY);
    if (attrName == null) {
      throw new IllegalStateException("attrName is not defined in ahead");
    }
    String attrNameString = ByteUtils.toString(attrName);
    // Prevent reading not existing attributes.
    if (!target.getRelation().getAttributeNames().contains(attrNameString)) {
      return length;
    }
    if (attrNameToCustomFormatter.containsKey(attrNameString)) {
      return attrNameToCustomFormatter.get(attrNameString).cutAndSet(in, offset, length, target);
    }
    target.putAttribute(attrNameString, in, offset, length);
    return length;
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    TreeBasedTupleSerializer tbs = (TreeBasedTupleSerializer) serializer;
    TreeNode<byte[]> parent = tbs.getCurrentParent();
    byte[] attr = findAttrName(parent);
    if (attr == null) {
      throw new SerdeException("attr name not found " + parent);
    }
    if (!Arrays.equals(MultiAttributeNamesFormatter.EMPTY_MARKER, attr)) {
      String attrName = ByteUtils.toString(attr);
      if (attrNameToCustomFormatter.containsKey(attrName)) {
        attrNameToCustomFormatter.get(attrName).accept(serializer, tuple);
        return;
      }
      AttributeType type = tuple.getAttributeType(attrName);
      byte[] byteVal = type.serialize(tuple.getAttribute(attrName));
      serializer.write(null, byteVal);
    } else {
      serializer.write(null, EMPTY_BYTES);
    }
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction) {
    serializer.write(null, TrueSegment.INSTANCE);
  }

  private byte[] findAttrName(TreeNode<byte[]> paraent) {
    String desc = paraent.getType();
    if (desc.startsWith(TreeNode.ATTRIBUTE_NAME_TYPE_PREFIX)) {
      return paraent.getValue();
    }
    if (paraent.getParent() == null) {
      return null;
    }
    return findAttrName(paraent.getParent());
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
      Map<String, Formatter> customFormatters =
          (Map<String, Formatter>) config.get(CUSTOM_FORMATTER_ATTRS_PROPKEY);
      return create(
          excludes == null ? Collections.emptyList() : (List<String>) excludes, customFormatters);
    }

    public MultiAttributeValuesFormatter create(List<String> excludes) {
      MultiAttributeValuesFormatter e = new MultiAttributeValuesFormatter();
      e.setExcludedAttributes(excludes);
      return e;
    }

    public MultiAttributeValuesFormatter create(
        List<String> excludes, Map<String, Formatter> attrNameToCustomFormatter) {
      MultiAttributeValuesFormatter e = create(excludes);
      if (attrNameToCustomFormatter == null) {
        return e;
      }
      e.attrNameToCustomFormatter = attrNameToCustomFormatter;
      return e;
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return MultiAttributeValuesFormatter.class;
    }
  }
}
