package jp.co.cyberagent.valor.sdk.serde;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.storage.Record;

public class OneToOneTupleSerializer implements TupleSerializer {

  private ByteArrayOutputStream baos;

  @Override
  public List<Record> serailize(Tuple tuple, List<FieldLayout> layouts) throws ValorException {
    Record record = new Record.RecordImpl();
    for (FieldLayout layout : layouts) {
      baos = new ByteArrayOutputStream();
      for (Segment f : layout.getFormatters()) {
        f.accept(this, tuple);
      }
      record.setBytes(layout.getFieldName(), baos.toByteArray());
    }
    return Arrays.asList(record);
  }

  @Override
  public void write(String type, byte[]... values) {
    for (byte[] v : values) {
      try {
        baos.write(v);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }
  }
}
