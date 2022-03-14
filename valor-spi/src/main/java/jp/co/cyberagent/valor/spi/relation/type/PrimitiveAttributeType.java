package jp.co.cyberagent.valor.spi.relation.type;

import java.util.Collections;
import java.util.List;

@SuppressWarnings("rawtypes")
public abstract class PrimitiveAttributeType<T> extends AttributeType<T> {

  @Override
  public List<AttributeType> getGenericParameterValues() {
    return Collections.emptyList();
  }

  @Override
  public void addGenericElementType(AttributeType attributeType) {
    throw new IllegalStateException(
        this.getClass().getCanonicalName() + " cannot have generic type parameters");
  }
}
