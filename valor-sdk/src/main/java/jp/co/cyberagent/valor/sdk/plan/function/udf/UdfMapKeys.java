package jp.co.cyberagent.valor.sdk.plan.function.udf;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.PluggableFactory;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.Udf;
import jp.co.cyberagent.valor.spi.relation.type.ArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;

public class UdfMapKeys implements Udf<List> {

  public static final String NAME = "map_keys";

  private ArrayAttributeType type;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void init(List<Expression> argExps) {
    if (argExps.size() != 1) {
      throw new IllegalArgumentException(NAME + " expects one argument");
    }
    Expression arg = argExps.get(0);
    AttributeType t = arg.getType();
    if (!(t instanceof MapAttributeType)) {
      throw new IllegalArgumentException(String.format("argument of %s should be a map", NAME));
    }
    this.type = ArrayAttributeType.create(((MapAttributeType) t).getKeyType());
  }

  @Override
  public List apply(Object... args) {
    Map m = (Map) args[0];
    return new ArrayList(m.keySet());
  }

  @Override
  public AttributeType<List> getReturnType() {
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
    UdfMapKeys that = (UdfMapKeys) o;
    return Objects.equals(type, that.type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type);
  }

  public static class Factory implements PluggableFactory<Udf, Void> {
    @Override
    public Udf create(Void config) {
      return new UdfMapKeys();
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Udf> getProvidedClass() {
      return UdfMapKeys.class;
    }
  }
}
