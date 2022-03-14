package jp.co.cyberagent.valor.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.type.Type;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.util.ByteUtils;

/**
 *
 */
public class ValorColumnHandle implements ColumnHandle {

  public static final String UPDATE_ROW_ID = "$update_row_id";

  public static boolean isUpdateRowIdColumn(ValorColumnHandle columnHandle) {
    return UPDATE_ROW_ID.equals(columnHandle.columnName);
  }

  public static byte[] toUpdateId(byte[]... keys) {
    int length = 0;
    for (byte[] k : keys) {
      length += ByteUtils.SIZEOF_INT;
      length += k.length;
    }
    ByteBuffer buf = ByteBuffer.allocate(length);
    for (byte[] k : keys) {
      buf.putInt(k.length);
      buf.put(k);
    }
    return buf.array();
  }

  public static Map<Relation.Attribute, Object> fromUpdateId(
      Relation.Attribute[] keyAttrs, byte[] updateId) {
    Map<Relation.Attribute, Object> t = new HashMap<>();
    try (ByteArrayInputStream bais = new ByteArrayInputStream(updateId);
         DataInputStream dis = new DataInputStream(bais)
    ) {
      for (int i = 0; i < keyAttrs.length; i++) {
        Relation.Attribute keyAttr = keyAttrs[i];
        int l = dis.readInt();
        byte[] b = new byte[l];
        dis.read(b);
        Object v = keyAttr.type().deserialize(b);
        t.put(keyAttr, v);
      }
      return t;
    } catch (IOException e) {
      throw new ValorRuntimeException(e);
    }

  }


  private final String connectorId;
  private final String columnName;
  private final Type columnType;

  @JsonCreator
  public ValorColumnHandle(@JsonProperty("connectorId") String connectorId,
                           @JsonProperty("columnName") String columnName,
                           @JsonProperty("columnType") Type columnType) {
    this.connectorId = connectorId;
    this.columnName = columnName;
    this.columnType = columnType;
  }

  @JsonProperty
  public String getConnectorId() {
    return connectorId;
  }

  @JsonProperty
  public String getColumnName() {
    return columnName;
  }

  @JsonProperty
  public Type getColumnType() {
    return columnType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValorColumnHandle that = (ValorColumnHandle) o;
    return Objects.equals(connectorId, that.connectorId) && Objects.equals(columnName,
        that.columnName) && Objects.equals(columnType, that.columnType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(connectorId, columnName, columnType);
  }
}
