package jp.co.cyberagent.valor.spi.serde;

import java.util.List;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.storage.Record;

public interface TupleDeserializer {

  void readRecord(List<String> fields, Record record) throws ValorException;

  void putAttribute(String attr, byte[] in, int offset, int length) throws SerdeException;

  void putAttribute(String attr, Object value) throws SerdeException;

  Object getAttribute(String attr);

  void setState(String name, byte[] in, int offset, int length);

  void setState(String name, byte[] value);

  byte[] getState(String name);

  Relation getRelation();

  Tuple pollTuple() throws ValorException;
}

