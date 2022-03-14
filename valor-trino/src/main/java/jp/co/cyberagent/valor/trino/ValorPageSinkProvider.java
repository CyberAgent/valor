package jp.co.cyberagent.valor.trino;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorOutputTableHandle;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.type.TypeManager;
import jp.co.cyberagent.valor.spi.ValorConnection;

/**
 *
 */
public class ValorPageSinkProvider implements ConnectorPageSinkProvider {

  private final ValorConnectorId connectorId;
  private final TypeManager typeManager;
  private ValorConnection connection;

  @Inject
  public ValorPageSinkProvider(ValorConnectorId connectorId, TypeManager typeManager) {
    this.connectorId = connectorId;
    this.typeManager = typeManager;
  }

  @Override
  public ConnectorPageSink createPageSink(ConnectorTransactionHandle connectorTransactionHandle,
                                          ConnectorSession connectorSession,
                                          ConnectorOutputTableHandle connectorOutputTableHandle) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConnectorPageSink createPageSink(ConnectorTransactionHandle connectorTransactionHandle,
                                          ConnectorSession connectorSession,
                                          ConnectorInsertTableHandle tableHandle) {
    return buildPageSink((ValorInsertTableHandle) tableHandle);
  }

  private ConnectorPageSink buildPageSink(ValorInsertTableHandle tableHandle) {
    return new ValorPageSink(connection, tableHandle, typeManager);
  }

  public void setConnection(ValorConnection connection) {
    this.connection = connection;
  }
}
