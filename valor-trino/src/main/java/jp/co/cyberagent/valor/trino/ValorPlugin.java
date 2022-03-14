package jp.co.cyberagent.valor.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import java.util.Collection;
import java.util.Set;

/**
 *
 */
public class ValorPlugin implements Plugin {

  private Collection<ConnectorFactory> connectorFactories =
      ImmutableList.of(new ValorConnectorFactory());

  public Iterable<ConnectorFactory> getConnectorFactories() {
    return connectorFactories;
  }

  @Override
  public Set<Class<?>> getFunctions() {
    return ImmutableSet.of(
        Murmur3Hash.class
    );
  }
}
