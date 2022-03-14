package jp.co.cyberagent.valor.sdk.plan.function.udf;

import java.util.Collection;
import java.util.List;
import jp.co.cyberagent.valor.spi.PluggableFactory;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.Udf;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.BooleanAttributeType;

public class UdfArrayContains implements Udf<Boolean> {

  public static final String NAME = "array_contains";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public void init(List<Expression> argExps) {
    // nothing to do
  }

  @Override
  public Boolean apply(Object... args) {
    Collection<Object> array = (List<Object>) args[0];
    return array.contains(args[1]);
  }

  @Override
  public AttributeType<Boolean> getReturnType() {
    return BooleanAttributeType.INSTANCE;
  }

  @Override
  public boolean equals(Object obj) {
    return obj.getClass() == this.getClass();
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  public static class Factory implements PluggableFactory<Udf, Void> {
    @Override
    public Udf create(Void config) {
      return new UdfArrayContains();
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Udf> getProvidedClass() {
      return UdfArrayContains.class;
    }
  }
}
