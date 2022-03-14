package jp.co.cyberagent.valor.spi.schema;

import java.util.Collection;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;

/**
 * Serialize/Deserialize each schema element
 */
public interface Segment {

  byte[] EMPTY_BYTES = new byte[0];

  enum Order {
    NORMAL,
    REVERSE,
    RANDOM
  }

  Order getOrder();

  /**
   * read relating values from the source and set the value to the target deserializer
   */
  int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target) throws SerdeException;

  void accept(TupleSerializer serializer, Tuple tuple) throws ValorException;

  void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws ValorException;

  boolean containsAttribute(String attr);

  String getName();

  Map<String, Object> getProperties();

  void setProperties(Map<String, Object> props);

  Formatter getFormatter();
}
