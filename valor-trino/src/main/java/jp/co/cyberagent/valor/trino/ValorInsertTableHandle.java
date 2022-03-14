package jp.co.cyberagent.valor.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;

/**
 *
 */
public class ValorInsertTableHandle extends ValorTableHandle implements ConnectorInsertTableHandle {

  private final List<ValorColumnHandle> columns;

  @JsonCreator
  public ValorInsertTableHandle(@JsonProperty("connectorId") String connectorId,
                                @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                                @JsonProperty("columns") List<ValorColumnHandle> columns
  ) {
    super(connectorId, schemaTableName);
    this.columns = columns;
  }

  @JsonProperty
  public List<ValorColumnHandle> getColumns() {
    return columns;
  }
}
