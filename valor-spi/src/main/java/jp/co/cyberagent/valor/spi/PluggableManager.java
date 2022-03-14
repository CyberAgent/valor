package jp.co.cyberagent.valor.spi;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public abstract class PluggableManager<T, C> {

  private Map<String, PluggableFactory<T, C>> factories = new HashMap<>();

  protected T create(String name, C config) {
    PluggableFactory<T, C> factory = factories.get(name);
    if (factory == null) {
      throw new IllegalArgumentException(name + " is not support");
    }
    try (ThreadContextClassLoader classLoader =
        new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
      return factory.create(config);
    }
  }

  public void addFactory(PluggableFactory<T, C> factory) {
    factory.getAliases().forEach(n -> factories.put(n, factory));
    Class<? extends T> cls = factory.getProvidedClass();
    factories.put(cls.getCanonicalName(), factory);
  }

  public abstract void installPlugin(Plugin plugin);
}
