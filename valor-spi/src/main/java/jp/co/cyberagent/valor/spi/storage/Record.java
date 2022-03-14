package jp.co.cyberagent.valor.spi.storage;

import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.InvalidFieldException;
import jp.co.cyberagent.valor.spi.exception.ValorException;

public interface Record {

  byte[] getBytes(String fieldName) throws ValorException;

  void setBytes(String fieldName, byte[] value) throws InvalidFieldException;

  class RecordImpl implements Record {

    private Map<String, byte[]> values = new HashMap<>();

    @Override
    public byte[] getBytes(String fieldName) {
      return values.get(fieldName);
    }

    @Override
    public void setBytes(String fieldName, byte[] value) {
      values.put(fieldName, value);
    }
  }
}
