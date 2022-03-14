package jp.co.cyberagent.valor.sdk.storage.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.InvalidFieldException;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.storage.Record;

/**
 *
 */
public class ResultSetRecord implements Record {

  private final ResultSet rs;
  private final Map<String, AttributeType> types;

  public ResultSetRecord(ResultSet rs, Map<String, AttributeType> types) {
    this.rs = rs;
    this.types = types;
  }

  @Override
  public byte[] getBytes(String fieldName) throws ValorException {
    AttributeType type = types.get(fieldName);
    if (type instanceof StringAttributeType) {
      try {
        return type.serialize(rs.getString(fieldName));
      } catch (SerdeException | SQLException e) {
        throw new ValorException(e);
      }
    }
    throw new UnsupportedOperationException(type + " is not supported in jdbc storage currently");
  }

  @Override
  public void setBytes(String fieldName, byte[] value) throws InvalidFieldException {
    throw new UnsupportedOperationException();
  }
}
