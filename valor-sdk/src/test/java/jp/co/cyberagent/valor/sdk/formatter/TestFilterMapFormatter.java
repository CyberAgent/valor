package jp.co.cyberagent.valor.sdk.formatter;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestFilterMapFormatter {

  private static final String MAP_ATTR = "m";
  private static final MapAttributeType mapType
      = MapAttributeType.create(StringAttributeType.INSTANCE, StringAttributeType.INSTANCE);
  private static final Relation relation = ImmutableRelation.builder()
      .relationId("testFilterMap")
      .addAttribute(MAP_ATTR, false, mapType).build();

  static class FilterMapFixture extends TupleFixture {
    public final Map<String, Object> config;

    FilterMapFixture(Relation relation, Map<String, Object> config,
                     Tuple tuple, byte[] recordValue, Tuple deseriazedTuple) {
      super(relation, tuple, recordValue, deseriazedTuple);
      this.config = config;
    }

    void test() throws Exception {
      FilterMapFormatter formatter = new FilterMapFormatter(config);
      testTupleFixture(formatter, this);
    }
  }


  static FilterMapFixture[] getFixtures() {
    Map<String, String> fullMap = new LinkedHashMap();
    fullMap.put("k2", "v2");
    fullMap.put("k1", "v1");
    Map<String, String> firstEntryMap = new LinkedHashMap();
    firstEntryMap.put("k1", "v1");

    Tuple fullTuple = new TupleImpl(relation);
    fullTuple.setAttribute(MAP_ATTR, fullMap);
    Tuple firstEntryTuple = new TupleImpl(relation);
    firstEntryTuple.setAttribute(MAP_ATTR, firstEntryMap);

    byte[] firstEntryBytes = ByteUtils.add(new byte[] {0x02}, ByteUtils.toBytes("k1"));
    firstEntryBytes = ByteUtils.add(firstEntryBytes, new byte[] {0x02}, ByteUtils.toBytes("v1"));

    Map<String, Object> inclusiveConf = new HashMap<String, Object>() {{
      put("attr", MAP_ATTR);
      put("include", Arrays.asList("k1"));
    }};

    return new FilterMapFixture[]{
        new FilterMapFixture(relation, inclusiveConf, fullTuple, firstEntryBytes, firstEntryTuple)
    };
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testSerde(FilterMapFixture fixture) throws Exception {
    fixture.test();
  }

  @Test
  public void testCollectFromMultipleFields() throws Exception {
    Map<String, Object> f1Conf = new HashMap<String, Object>() {{
      put("attr", "m");
      put("include", Arrays.asList("k1"));
    }};
    FilterMapFormatter f1Serde = new FilterMapFormatter(f1Conf);

    Map<String, Object> f2Conf = new HashMap<String, Object>() {{
      put("attr", "m");
      put("include", Arrays.asList("k2"));
    }};
    FilterMapFormatter f2Serde = new FilterMapFormatter(f2Conf);

    byte[] f1Bytes = ByteUtils.add(new byte[] {0x02}, ByteUtils.toBytes("k1"));
    f1Bytes = ByteUtils.add(f1Bytes, new byte[] {0x02}, ByteUtils.toBytes("v1"));

    byte[] f2Bytes = ByteUtils.add(new byte[] {0x02}, ByteUtils.toBytes("k2"));
    f2Bytes = ByteUtils.add(f2Bytes, new byte[] {0x02}, ByteUtils.toBytes("v2"));

    Record record = new Record.RecordImpl();
    record.setBytes("f1", f1Bytes);
    record.setBytes("f2", f2Bytes);

    TupleDeserializer deserializer
        = new OneToOneDeserializer(relation, FieldLayout.of("f1", f1Serde), FieldLayout.of("f2", f2Serde));
    deserializer.readRecord(Arrays.asList("f1", "f2"), record);
    Tuple t = deserializer.pollTuple();
    Map m = (Map) t.getAttribute("m");
    assertEquals(2, m.size());
    assertEquals("v1", m.get("k1"));
    assertEquals("v2", m.get("k2"));
  }
}
