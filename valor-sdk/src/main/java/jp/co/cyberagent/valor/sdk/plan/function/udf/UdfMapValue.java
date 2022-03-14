package jp.co.cyberagent.valor.sdk.plan.function.udf;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.PluggableFactory;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.Udf;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;

public class UdfMapValue implements Udf<Object> {

  public static final String NAME = "map_value";

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
    if (!(t instanceof MapAttributeType)) {
      throw new IllegalArgumentException(String.format("1st argument of %s should be a map", NAME));
    }
    MapAttributeType mapType = (MapAttributeType) t;
    this.type = mapType.getValueType();
  }

  @Override
  public Object apply(Object... args) {
    Map m = (Map) args[0];
    Object k = args[1];
    String.format("eval map value (%s, %s) = ", m, k, m.get(k));
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
    UdfMapValue that = (UdfMapValue) o;
    return Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  public static class Factory implements PluggableFactory<Udf, Void> {
    @Override
    public Udf create(Void config) {
      return new UdfMapValue();
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Udf> getProvidedClass() {
      return UdfMapValue.class;
    }
  }
}
