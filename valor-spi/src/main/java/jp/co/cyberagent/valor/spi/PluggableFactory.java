package jp.co.cyberagent.valor.spi;

import java.util.Arrays;
import java.util.Collection;

public interface PluggableFactory<T, C> {

  T create(C config);

  default Collection<String> getAliases() {
    return Arrays.asList(getName());
  }

  String getName();

  Class<? extends T> getProvidedClass();
}
