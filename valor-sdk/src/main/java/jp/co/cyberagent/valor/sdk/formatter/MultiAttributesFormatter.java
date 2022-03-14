package jp.co.cyberagent.valor.sdk.formatter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MultiAttributesFormatter extends DisassembleFormatter {

  public static final Logger LOG = LoggerFactory.getLogger(MultiAttributesFormatter.class);

  public static final String EXCLUDED_ATTRS_PROPKEY = "excludes";

  protected List<String> excludedAttributes;

  protected SortedSet<String> getTargetAttributes(Tuple value) {
    SortedSet<String> set = new TreeSet<>();
    value.getAttributeNames().stream().filter(this::containsAttribute).forEach(set::add);
    return set;
  }

  protected void setExcludedAttributes(List<String> attrs) {
    this.excludedAttributes = attrs;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> props = new HashMap<>();
    props.put(EXCLUDED_ATTRS_PROPKEY, excludedAttributes);
    return props;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void setProperties(Map<String, Object> props) {
    this.excludedAttributes = (List<String>) props.get(EXCLUDED_ATTRS_PROPKEY);
  }

  @Override
  public boolean containsAttribute(String attr) {
    return !excludedAttributes.contains(attr);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MultiAttributesFormatter that = (MultiAttributesFormatter) o;
    return Objects.equals(excludedAttributes, that.excludedAttributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(excludedAttributes);
  }
}
