package jp.co.cyberagent.valor.sdk.serde;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TupleDeserializerBase implements TupleDeserializer {

  static Logger LOG = LoggerFactory.getLogger(TupleDeserializerBase.class);

  protected final Relation relation;
  protected final List<FieldLayout> layouts;
  protected final Map<String, byte[]> state;

  public TupleDeserializerBase(Relation relation, List<FieldLayout> layouts) {
    this.relation = relation;
    this.layouts = layouts;
    this.state = new HashMap<>();
  }

  @Override
  public void readRecord(List<String> fields, Record record) throws ValorException {
    for (FieldLayout formatter : layouts) {
      if (fields.contains(formatter.fieldName())) {
        deserializeField(formatter, record);
      }
    }
    state.clear();
  }

  private void deserializeField(FieldLayout formatter, Record record) throws ValorException {
    byte[] value = record.getBytes(formatter.fieldName());
    int position = 0;
    int length = value.length;
    for (Segment elm : formatter.formatters()) {
      try {
        int read = elm.cutAndSet(value, position, length, this);
        position += read;
        length -= read;
      } catch (Exception e) {
        String msg = String.format(
            "failed to read %s.%s of %s", formatter.fieldName(), elm, record);
        if (e instanceof SerdeException) {
          throw new SerdeException(msg, e);
        } else {
          throw new ValorException(msg, e);
        }
      }
    }
  }

  @Override
  public void setState(String name, byte[] in, int offset, int length) {
    setState(name, ByteUtils.copy(in, offset, length));
  }

  @Override
  public void setState(String name, byte[] value) {
    state.put(name, value);
  }

  @Override
  public byte[] getState(String name) {
    return state.get(name);
  }

  @Override
  public Relation getRelation() {
    return relation;
  }
}
