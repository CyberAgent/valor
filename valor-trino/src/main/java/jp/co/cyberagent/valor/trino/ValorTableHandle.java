package jp.co.cyberagent.valor.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import java.util.Objects;

/**
 *
 */
public class ValorTableHandle implements ConnectorTableHandle {

  private final String connectorId;
  private SchemaTableName schemaTableName;

  @JsonCreator
  public ValorTableHandle(@JsonProperty("connectorId") String connectorId,
                          @JsonProperty("schemaTableName") SchemaTableName tableName) {
    this.connectorId = connectorId;
    this.schemaTableName = tableName;
  }

  @JsonProperty
  public String getConnectorId() {
    return connectorId;
  }

  @JsonProperty
  public SchemaTableName getSchemaTableName() {
    return schemaTableName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValorTableHandle that = (ValorTableHandle) o;
    return Objects.equals(connectorId, that.connectorId)
        && Objects.equals(schemaTableName, that.schemaTableName);
  }

  @Override
  public int hashCode() {

    return Objects.hash(connectorId, schemaTableName);
  }
}
