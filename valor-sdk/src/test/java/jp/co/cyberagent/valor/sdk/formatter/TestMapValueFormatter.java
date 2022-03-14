package jp.co.cyberagent.valor.sdk.formatter;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.holder.FixedLengthHolder;
import jp.co.cyberagent.valor.sdk.serde.ContinuousRecordsDeserializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestMapValueFormatter {

  public static final String FIELD_NAME = "f";
  private MapAttributeType mapAttributeType;
  private Relation relation;

  @BeforeEach
  public void init() {
    mapAttributeType = new MapAttributeType();
    mapAttributeType.addGenericElementType(StringAttributeType.INSTANCE);
    mapAttributeType.addGenericElementType(LongAttributeType.INSTANCE);

    relation = ImmutableRelation.builder()
        .relationId("r")
        .addAttribute("m", false, mapAttributeType)
        .build();
  }

  @Test
  public void testDefaultSetting() throws Exception {
    Map<String, Long> m = new LinkedHashMap();
    m.put("k1", 200l);
    Tuple t = new TupleImpl(relation);
    t.setAttribute("m", m);

    byte[] expectedKey = ByteUtils.toBytes("k1");
    byte[] expectedValue = ByteUtils.toBytes(200l);

    MapKeyFormatter keyFormatter = new MapKeyFormatter("m");
    MapValueFormatter valueFormatter = new MapValueFormatter("m");
    FieldLayout layout = FieldLayout.of(
        FIELD_NAME, FixedLengthHolder.create(expectedKey.length, keyFormatter), valueFormatter);

    TreeBasedTupleSerializer serializer = new TreeBasedTupleSerializer();
    List<Record> records = serializer.serailize(t, Arrays.asList(layout));
    assertArrayEquals(ByteUtils.add(expectedKey, expectedValue), records.get(0).getBytes(FIELD_NAME));

    ContinuousRecordsDeserializer deserializer
        = new ContinuousRecordsDeserializer(relation, Arrays.asList(layout));
    deserializer.readRecord(Arrays.asList(FIELD_NAME), records.get(0));
    t = deserializer.flushRemaining();
    Map<String, String> actualMap = (Map<String, String>) t.getAttribute("m");
    assertEquals(1, actualMap.size());
    assertEquals(200l, actualMap.get("k1"));
  }

}


