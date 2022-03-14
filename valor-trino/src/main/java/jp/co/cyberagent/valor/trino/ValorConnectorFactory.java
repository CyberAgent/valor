package jp.co.cyberagent.valor.trino;

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorHandleResolver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ValorConnectorFactory implements ConnectorFactory {

  static Logger LOG = LoggerFactory.getLogger(ValorConnectorFactory.class);

  static final String NAMESPACE_CONF_KEY = "namespaces";

  @Override
  public String getName() {
    return "valor";
  }

  @Override
  public ConnectorHandleResolver getHandleResolver() {
    return new ValorHandleResolver();
  }

  @Override
  public Connector create(String connectorId, Map<String, String> config,
                          ConnectorContext connectorContext) {
    // original config is immutable, wrap with hash map to remove namespace parameters
    config = new HashMap<>(config);
    String namespaces = config.remove(NAMESPACE_CONF_KEY);
    Map<String, ValorConf> namespaceConfigs = new HashMap<>();
    if (namespaces != null) {
      for (String namespace : namespaces.split(",")) {
        List<String> keys = config.keySet().stream()
            .filter(k -> k.startsWith(namespace)).collect(Collectors.toList());
        ValorConf nsConf = new ValorConfImpl();
        for (String key : keys) {
          nsConf.set(key.substring(namespace.length() + 1), config.remove(key));
        }
        namespaceConfigs.put(namespace, nsConf);
      }
    }

    ValorConf conf = new ValorConfImpl(config);
    ValorContext context = StandardContextFactory.create(conf);
    Bootstrap app = new Bootstrap(new JsonModule(),
        new ValorModule(connectorId, connectorContext.getTypeManager(), context, namespaceConfigs));
    try {
      Injector injector =
          app.doNotInitializeLogging().setOptionalConfigurationProperties(config).nonStrictConfig()
              .initialize();
      return injector.getInstance(ValorConnector.class);
    } catch (Exception e) {
      LOG.error("failed to create connection", e);
      throw new RuntimeException(e);
    }
  }
}
