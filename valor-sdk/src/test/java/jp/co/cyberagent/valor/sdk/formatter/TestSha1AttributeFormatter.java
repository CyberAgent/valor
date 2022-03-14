package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestSha1AttributeFormatter {

  private static final int LENGTH = 5;
  private Sha1AttributeFormatter element = Sha1AttributeFormatter.create("test", LENGTH);
  private Relation relation = ImmutableRelation.builder().relationId("test")
      .addAttribute("test",true, StringAttributeType.INSTANCE).build();

  @Test
  public void testSerDes() throws Exception {
    String prehash = "prehash";
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute("test", prehash);
    byte[] expected = new byte[] {
        56, 49, 53, 57, 55, 50, 55, 55, 52, 98, 99, 99, 57, 53, 100, 52, 101, 53, 54, 102, 102, 51,
        56, 49, 101, 52, 54, 48, 101, 55, 101, 54, 100, 51, 49, 54, 54, 56, 55, 97};

    expected = Arrays.copyOf(expected, LENGTH);

    TupleSerializer serializer = mock(TupleSerializer.class);
    element.accept(serializer, tuple);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));

    TupleDeserializer deserializer = new OneToOneDeserializer(relation, "", element);
    setField(deserializer, "tuple", new TupleImpl(relation));
    byte[] source = ByteUtils.add(expected, ByteUtils.toBytes(prehash));
    int l = element.cutAndSet(source, 0, source.length, deserializer);
    assertEquals(LENGTH, l);
    tuple = deserializer.pollTuple();
    // hash formatter cannot keep its value;
    assertNull(tuple.getAttribute("test"));
    setField(deserializer, "tuple", new TupleImpl(relation));
    new AttributeValueFormatter("test").cutAndSet(source, l, source.length - l, deserializer);
    tuple = deserializer.pollTuple();
    assertEquals(prehash, tuple.getAttribute("test"));
  }

  @Test
  public void testScan() throws Exception {
    String value = "prehash";
    EqualOperator ep = new EqualOperator("test", StringAttributeType.INSTANCE, value);

    QuerySerializer serializer = mock(QuerySerializer.class);
    element.accept(serializer, Arrays.asList(ep));
    byte[] expected = new byte[] {56, 49, 53, 57, 55, 50, 55, 55, 52, 98, 99, 99, 57, 53, 100, 52, 101, 53, 54, 102, 102, 51, 56, 49, 101, 52, 54, 48, 101, 55, 101, 54, 100, 51, 49, 54, 54, 56, 55, 97};
    expected = Arrays.copyOf(expected, LENGTH);
    verify(serializer).write(null, new CompleteMatchSegment(null, expected));
  }
}
