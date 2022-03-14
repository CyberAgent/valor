package jp.co.cyberagent.valor.spi.relation;

import java.util.Collection;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

public interface Tuple {

  /**
   * get value of attribute
   */
  Object getAttribute(String attr);

  AttributeType getAttributeType(String attr);

  /**
   * set value of attribute
   */
  void setAttribute(String attr, Object value);

  /**
   * get list of attributes
   */
  Collection<String> getAttributeNames();

  Tuple getCopy();
}
