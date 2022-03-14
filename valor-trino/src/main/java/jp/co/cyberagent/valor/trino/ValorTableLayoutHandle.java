package jp.co.cyberagent.valor.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import java.util.Objects;

/**
 *
 */
public class ValorTableLayoutHandle implements ConnectorTableLayoutHandle {

  private final String connectorId;
  private final TupleDomain<ColumnHandle> constraint;
  private final SchemaTableName schemaTableName;

  @JsonCreator
  public ValorTableLayoutHandle(@JsonProperty("connectorId") String connectorId,
                                @JsonProperty("schemaTableName") SchemaTableName tableName,
                                @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint) {
    this.connectorId = connectorId;
    this.schemaTableName = tableName;
    this.constraint = constraint;
  }

  @JsonProperty
  public String getConnectorId() {
    return connectorId;
  }

  @JsonProperty
  public SchemaTableName getSchemaTableName() {
    return schemaTableName;
  }

  @JsonProperty
  public TupleDomain<ColumnHandle> getConstraint() {
    return constraint;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValorTableLayoutHandle that = (ValorTableLayoutHandle) o;
    return Objects.equals(connectorId, that.connectorId)
        && Objects.equals(constraint, that.constraint)
        && Objects.equals(schemaTableName, that.schemaTableName);
  }

  @Override
  public int hashCode() {

    return Objects.hash(connectorId, constraint, schemaTableName);
  }
}
