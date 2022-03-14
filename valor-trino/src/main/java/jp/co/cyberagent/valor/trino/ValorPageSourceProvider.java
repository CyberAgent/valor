package jp.co.cyberagent.valor.trino;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.type.TypeManager;
import java.util.List;
import jp.co.cyberagent.valor.spi.ValorConnection;

public class ValorPageSourceProvider implements ConnectorPageSourceProvider {

  private final ValorConnectorId connectorId;
  private final TypeManager typeManager;
  private final ValorRecordSetProvider recordSetProvider;
  private ValorConnection connection;

  @Inject
  public ValorPageSourceProvider(ValorConnectorId connectorId,
                                 TypeManager typeManager,
                                 ValorRecordSetProvider recordSetProvider) {
    this.connectorId = connectorId;
    this.typeManager = typeManager;
    this.recordSetProvider = recordSetProvider;
  }

  @Override
  public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
                                              ConnectorSession session,
                                              ConnectorSplit split,
                                              ConnectorTableHandle table,
                                              List<ColumnHandle> columns,
                                              DynamicFilter dynamicFilter) {
    ValorRecordSet recordSet = (ValorRecordSet) recordSetProvider
        .getRecordSet(transaction, session, split, table, columns);
    if (isUpdate(columns)) {
      return new ValorUpdatablePageSource(connection, (ValorTableHandle) table, recordSet);
    }
    return new RecordPageSource(recordSet);
  }

  private boolean isUpdate(List<ColumnHandle> columns) {
    for (ColumnHandle column : columns) {
      ValorColumnHandle vch = (ValorColumnHandle) column;
      if (this.connectorId.equals(connectorId)
          && ValorColumnHandle.UPDATE_ROW_ID.equals(vch.getColumnName())) {
        return true;
      }
    }
    return false;
  }

  public void setConnection(ValorConnection connection) {
    this.connection = connection;
    this.recordSetProvider.setConnection(connection);
  }
}
