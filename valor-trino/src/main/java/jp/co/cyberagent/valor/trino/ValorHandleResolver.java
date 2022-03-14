package jp.co.cyberagent.valor.trino;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorHandleResolver;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;

/**
 *
 */
public class ValorHandleResolver implements ConnectorHandleResolver {
  @Override
  public Class<? extends ConnectorTableHandle> getTableHandleClass() {
    return ValorTableHandle.class;
  }

  @Override
  public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
    return ValorTableLayoutHandle.class;
  }

  @Override
  public Class<? extends ColumnHandle> getColumnHandleClass() {
    return ValorColumnHandle.class;
  }

  @Override
  public Class<? extends ConnectorSplit> getSplitClass() {
    return ValorSplit.class;
  }

  @Override
  public Class<? extends ConnectorTransactionHandle> getTransactionHandleClass() {
    return ValorTransactionHandle.class;
  }

  @Override
  public Class<? extends ConnectorInsertTableHandle> getInsertTableHandleClass() {
    return ValorInsertTableHandle.class;
  }
}
