package jp.co.cyberagent.valor.sdk.plan.function.udf;

import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.PluggableFactory;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.Udf;
import jp.co.cyberagent.valor.spi.relation.type.ArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

public class UdfArrayValue implements Udf<Object> {

  public static final String NAME = "array_value";

  private AttributeType type;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void init(List<Expression> argExps) {
    if (argExps.size() != 2) {
      throw new IllegalArgumentException(NAME + " expect 2 arguments");
    }
    AttributeType t = argExps.get(0).getType();
    if (!(t instanceof ArrayAttributeType)) {
      throw new IllegalArgumentException(
          String.format("1st argument of %s should be an array", NAME));
    }
    this.type = ((ArrayAttributeType) t).getElementType();
  }

  @Override
  public Object apply(Object... args) {
    List m = (List) args[0];
    int k = (int) args[1];
    return m.get(k);
  }

  @Override
  public AttributeType getReturnType() {
    return type;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UdfArrayValue that = (UdfArrayValue) o;
    return Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  public static class Factory implements PluggableFactory<Udf, Void> {
    @Override
    public Udf create(Void config) {
      return new UdfArrayValue();
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Udf> getProvidedClass() {
      return UdfArrayValue.class;
    }
  }
}
