package jp.co.cyberagent.valor.trino;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.transaction.IsolationLevel;
import java.util.List;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ValorConnector implements Connector {

  public static final String TABLE_PROPERTY_KEYS = "keys";
  public static final String TABLE_PROPERTY_SCHEMA = "schema";
  @SuppressWarnings("unchecked")
  static final List<PropertyMetadata<?>> tableProperties =
      new ImmutableList.Builder().add(PropertyMetadata
          .stringProperty(TABLE_PROPERTY_KEYS, "key attributes (comma separated)", null, false))
          .add(PropertyMetadata
              .stringProperty(TABLE_PROPERTY_SCHEMA, "schema definition in json", null, false))
          .build();
  static Logger LOG = LoggerFactory.getLogger(ValorConnector.class);
  private final ValorMetadata metadata;
  private final ValorSplitManager splitManager;
  private final ValorPageSinkProvider pageSinkProvider;
  private final ValorPageSourceProvider pageSourceProvider;
  private final LifeCycleManager lifeCycleManager;
  private ValorConnection connection;

  @Inject
  public ValorConnector(ValorContext context,
                        LifeCycleManager lifeCycleManager,
                        ValorMetadata metadata,
                        ValorSplitManager splitManager,
                        ConnectorPageSinkProvider pageSinkProvider,
                        ConnectorPageSourceProvider pageSourceProvider) {
    this.lifeCycleManager = lifeCycleManager;
    initConnection(context);
    this.metadata = requireNonNull(metadata, "metadata is null");
    this.metadata.setConnection(connection);
    this.splitManager = requireNonNull(splitManager, "splitManager is null");
    this.splitManager.setConnection(connection);
    this.pageSinkProvider = (ValorPageSinkProvider) pageSinkProvider;
    this.pageSinkProvider.setConnection(connection);
    this.pageSourceProvider = (ValorPageSourceProvider) pageSourceProvider;
    this.pageSourceProvider.setConnection(connection);
  }

  private void initConnection(ValorContext context) {
    try {
      this.connection = ValorConnectionFactory.create(context);
    } catch (Exception e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
  }

  @Override
  public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean b) {
    return ValorTransactionHandle.INSTANCE;
  }

  @Override
  public ConnectorMetadata getMetadata(ConnectorTransactionHandle connectorTransactionHandle) {
    return metadata;
  }

  @Override
  public ConnectorSplitManager getSplitManager() {
    return splitManager;
  }

  @Override
  public ConnectorPageSinkProvider getPageSinkProvider() {
    return pageSinkProvider;
  }

  @Override
  public ConnectorPageSourceProvider getPageSourceProvider() {
    return pageSourceProvider;
  }

  @Override
  public List<PropertyMetadata<?>> getTableProperties() {
    return tableProperties;
  }

  @Override
  public void shutdown() {
    try {
      connection.close();
      lifeCycleManager.stop();
    } catch (Exception e) {
      LOG.error("Error shutting down connector", e);
    }
  }
}
