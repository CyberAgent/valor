package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;

public class TupleFixture {

  public final Relation relation;
  public final Tuple tuple;
  public final byte[] recordValue;
  public final Tuple deserializedTuple;

  public TupleFixture(Relation relation, Tuple tuple, byte[] recordValue) {
    this(relation, tuple, recordValue, tuple);
  }

  public TupleFixture(Relation relation, Tuple tuple, byte[] recordValue,
                      Tuple  deserializedTuple) {
    this.relation = relation;
    this.tuple = tuple;
    this.recordValue = recordValue;
    this.deserializedTuple = deserializedTuple;
  }

  public static void testTupleFixture(Formatter formatter, TupleFixture fixture) throws Exception {
    if (fixture.tuple != null) {
      TupleSerializer serializer = mock(TupleSerializer.class);
      formatter.accept(serializer, fixture.tuple);
      verify(serializer).write(isNull(String.class), argThat(equalBytes(fixture.recordValue)));
    }

    if (fixture.deserializedTuple != null) {
      TupleDeserializer deserializer = new OneToOneDeserializer(fixture.relation, "", formatter);
      setField(deserializer, "tuple", new TupleImpl(fixture.relation));
      formatter.cutAndSet(fixture.recordValue, 0, fixture.recordValue.length, deserializer);
      Tuple tuple = deserializer.pollTuple();
      for (String attr : fixture.relation.getAttributeNames()) {
        assertEquals(tuple.getAttribute(attr), fixture.deserializedTuple.getAttribute(attr));
      }
    }

  }

}
