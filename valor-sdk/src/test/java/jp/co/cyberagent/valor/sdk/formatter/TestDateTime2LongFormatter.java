package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestDateTime2LongFormatter {

  private static final String ATTR_NAME = "attr";
  private static final String PATTERN_PROPKEY = "pattern";
  private static final String PATTERNS_PROPKEY = "patterns";
  private static final String ORDER_PROPKEY = "order";
  private static final String TIMEZONE_PROPKEY = "timezone";

  private static final long NANOSECONDS_PER_SECONDS = 1000000000L;

  private static final long unixtime = 1412916640000000000l;
  private static final long reversedUnixTime = Long.MAX_VALUE - unixtime;
  private static final String ISO_ZONED_DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX";
  private static final String ISO_LOCAL_DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";

  private final String iso_zoned_date = Instant.ofEpochSecond(
      unixtime / NANOSECONDS_PER_SECONDS, unixtime % NANOSECONDS_PER_SECONDS)
          .atZone(TimeZone.getDefault().toZoneId())
          .format(DateTimeFormatter.ofPattern(ISO_ZONED_DATE_TIME_PATTERN));
  private final String iso_zoned_date_utc = Instant.ofEpochSecond(
      unixtime / NANOSECONDS_PER_SECONDS, unixtime % NANOSECONDS_PER_SECONDS)
          .atZone(TimeZone.getTimeZone("UTC").toZoneId())
          .format(DateTimeFormatter.ofPattern(ISO_ZONED_DATE_TIME_PATTERN));

  private final String iso_local_date_time = Instant.ofEpochSecond(
      unixtime / NANOSECONDS_PER_SECONDS, unixtime % NANOSECONDS_PER_SECONDS)
          .atZone(TimeZone.getDefault().toZoneId())
          .format(DateTimeFormatter.ofPattern(ISO_LOCAL_DATE_TIME_PATTERN));
  private final String iso_local_date_time_utc = Instant.ofEpochSecond(
      unixtime / NANOSECONDS_PER_SECONDS, unixtime % NANOSECONDS_PER_SECONDS)
          .atZone(TimeZone.getTimeZone("UTC").toZoneId())
          .format(DateTimeFormatter.ofPattern(ISO_LOCAL_DATE_TIME_PATTERN));

  private Map<String, Object> config = new HashMap<String, Object>() {
    {
      put(ATTR_NAME, "attr");
      put(PATTERN_PROPKEY, ISO_ZONED_DATE_TIME_PATTERN);
    }
  };
  private Formatter serde = new DateTime2LongFormatter(config);
  private Relation relation =
      ImmutableRelation.builder().relationId("test").addAttribute(ATTR_NAME, true,
          StringAttributeType.INSTANCE).build();

  // record with default timezone can be parsed
  @Test
  public void testSerialize() throws Exception {
    byte[] expected = ByteUtils.toBytes(unixtime);
    TupleSerializer serializer = mock(TupleSerializer.class);
    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR_NAME, iso_zoned_date);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }

  // record can be parsed when record and dateTimeFormatter have UTC timezone
  @Test
  public void testSerializeUTC() throws Exception {
    byte[] expected = ByteUtils.toBytes(unixtime);
    TupleSerializer serializer = mock(TupleSerializer.class);

    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR_NAME, iso_zoned_date);
    config.put(TIMEZONE_PROPKEY, "UTC");
    Formatter serde = new DateTime2LongFormatter(config);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }

  // record timezone overwrites dateTimeFormatter timezone
  @Test
  public void testSerializeDifferentTz() throws Exception {
    byte[] expected = ByteUtils.toBytes(unixtime);
    TupleSerializer serializer = mock(TupleSerializer.class);
    Tuple t = new TupleImpl(relation);
    config.put(TIMEZONE_PROPKEY, "Asia/Tokyo");
    serde = new DateTime2LongFormatter(config);
    t.setAttribute(ATTR_NAME, iso_zoned_date_utc);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }

  // record without timezone can be parsed if the dateTimeFormatter has timezone
  @Test
  public void testSerializeNoRecordTz() throws Exception {
    byte[] expected = ByteUtils.toBytes(unixtime);
    TupleSerializer serializer = mock(TupleSerializer.class);
    Tuple t = new TupleImpl(relation);
    config.put(PATTERN_PROPKEY, ISO_LOCAL_DATE_TIME_PATTERN);
    config.put(TIMEZONE_PROPKEY, "UTC");
    serde = new DateTime2LongFormatter(config);
    t.setAttribute(ATTR_NAME, iso_local_date_time_utc);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }

  // record can be parsed as long as record has timezone
  @Test
  public void testSerializeNoDtfTz() throws Exception {
    byte[] expected = ByteUtils.toBytes(unixtime);
    TupleSerializer serializer = mock(TupleSerializer.class);
    Tuple t = new TupleImpl(relation);
    t.setAttribute(ATTR_NAME, iso_zoned_date_utc);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }

  // if no timezone specified, use system default timezone
  @Test
  public void testSerializeNoRecordTzNoDtfTz() throws Exception {
    byte[] expected = ByteUtils.toBytes(unixtime);
    TupleSerializer serializer = mock(TupleSerializer.class);
    Tuple t = new TupleImpl(relation);
    config.put(PATTERN_PROPKEY, ISO_LOCAL_DATE_TIME_PATTERN);
    serde = new DateTime2LongFormatter(config);
    t.setAttribute(ATTR_NAME, iso_local_date_time);
    serde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }

  @Test
  public void testConvertNanos() throws Exception {
    TupleSerializer serializer = mock(TupleSerializer.class);
    Tuple t = new TupleImpl(relation);

    List<String> patterns = new ArrayList<>();
    patterns.add("yyyy-MM-dd'T'HH:mm:ss[.SSSSSSSSS]XXX");
    patterns.add("yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]XXX");
    patterns.add("yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX");

    Map<Long, String> records4Serialize = new HashMap<Long, String>();
    records4Serialize.put(1412916640001000000L, "2014-10-10T13:50:40.001+09:00");
    records4Serialize.put(1412916640001002000L, "2014-10-10T13:50:40.001002+09:00");
    records4Serialize.put(1412916640001002003L, "2014-10-10T13:50:40.001002003+09:00");

    Map<Long, String> records4Deserialize = new HashMap<Long, String>();
    records4Deserialize.put(1412916640001000000L, "2014-10-10T18:50:40.001000000+14:00");
    records4Deserialize.put(1412916640001002000L, "2014-10-10T18:50:40.001002000+14:00");
    records4Deserialize.put(1412916640001002003L, "2014-10-10T18:50:40.001002003+14:00");

    Map<String, Object> conf = new HashMap<>();
    conf.put(ATTR_NAME, "attr");
    conf.put(PATTERNS_PROPKEY, patterns);
    conf.put(TIMEZONE_PROPKEY, "Pacific/Apia");

    serde = new DateTime2LongFormatter(conf);

    /// test serialize
    for (Map.Entry<Long, String> entry : records4Serialize.entrySet()) {
      t.setAttribute(ATTR_NAME, entry.getValue());
      serde.accept(serializer, t);
      verify(serializer, atLeastOnce()).write(isNull(String.class),
          argThat(equalBytes(ByteUtils.toBytes(entry.getKey()))));
    }

    // test deserialize
    for(Map.Entry<Long, String> entry : records4Deserialize.entrySet()) {
      TupleDeserializer deserializer = new OneToOneDeserializer(relation);
      setField(deserializer, "tuple", new TupleImpl(relation));
      byte[] bytes = ByteUtils.toBytes(entry.getKey());
      serde.cutAndSet(bytes, 0, bytes.length, deserializer);
      t = deserializer.pollTuple();
      assertEquals(entry.getValue(), t.getAttribute(ATTR_NAME));
    }
  }

  @Test
  public void testVariousPatterns() throws Exception {
    byte[] expected = ByteUtils.toBytes(unixtime);
    TupleSerializer serializer = mock(TupleSerializer.class);
    Tuple t = new TupleImpl(relation);

    List<String> patterns = new ArrayList<>();
    patterns.add("yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX");
    patterns.add("yyyy-MM-dd'T'HH:mm:ss[.S]XXX");
    patterns.add("yyyy-MM-ddHH:mm:ss[.SSS]XXX");
    patterns.add("yyyy-MM-ddHH:mm:ss[.SSS]X");

    List<String> records = new ArrayList<>();
    records.add("2014-10-10T13:50:40.000+09:00");
    records.add("2014-10-10T13:50:40.0+09:00");
    records.add("2014-10-1013:50:40.000+09:00");
    records.add("2014-10-1013:50:40.000+09");

    Map<String, Object> conf = new HashMap<>();
    conf.put(ATTR_NAME, "attr");
    conf.put(PATTERNS_PROPKEY, patterns);
    conf.put(TIMEZONE_PROPKEY, "Pacific/Apia");

    serde = new DateTime2LongFormatter(conf);

    for (String r : records) {
      t.setAttribute(ATTR_NAME, r);
      serde.accept(serializer, t);
      verify(serializer, atLeastOnce()).write(isNull(String.class), argThat(equalBytes(expected)));
    }

    // test deserialize
    TupleDeserializer deserializer = new OneToOneDeserializer(relation);
    setField(deserializer, "tuple", new TupleImpl(relation));
    byte[] bytes = ByteUtils.toBytes(unixtime);
    serde.cutAndSet(bytes, 0, bytes.length, deserializer);
    t = deserializer.pollTuple();
    assertEquals("2014-10-10T18:50:40.000+14:00", t.getAttribute(ATTR_NAME));
  }

  @Test
  public void testSerializeReverse() throws Exception {
    Tuple t = new TupleImpl(relation);
    byte[] expected = ByteUtils.toBytes(reversedUnixTime);

    config.put(ORDER_PROPKEY, "desc");
    Formatter reversedSerde = new DateTime2LongFormatter(config);

    TupleSerializer serializer = mock(TupleSerializer.class);
    t.setAttribute(ATTR_NAME, iso_zoned_date);
    reversedSerde.accept(serializer, t);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }

  // use systemDefault timezone when the dateTimeFormatter timezone is not set
  @Test
  public void testDeserializeSystemDefaultTZ() throws Exception {
    TupleDeserializer f = new OneToOneDeserializer(relation);
    setField(f, "tuple", new TupleImpl(relation));
    byte[] bytes = ByteUtils.toBytes(unixtime);
    serde.cutAndSet(bytes, 0, bytes.length, f);
    Tuple t = f.pollTuple();
    assertEquals(iso_zoned_date, t.getAttribute(ATTR_NAME));
  }

  // use dateTimeFormatter timezone when the dateTimeFormatter is set
  @Test
  public void testDeserializeRecordTZ() throws Exception {
    TupleDeserializer f = new OneToOneDeserializer(relation);
    setField(f, "tuple", new TupleImpl(relation));
    byte[] bytes = ByteUtils.toBytes(unixtime);
    config.put(TIMEZONE_PROPKEY, "UTC");
    serde = new DateTime2LongFormatter(config);
    serde.cutAndSet(bytes, 0, bytes.length, f);
    Tuple t = f.pollTuple();
    assertEquals(iso_zoned_date_utc, t.getAttribute(ATTR_NAME));
  }

  @Test
  public void testDeserializeReverse() throws Exception {
    TupleDeserializer f = new OneToOneDeserializer(relation);
    setField(f, "tuple", new TupleImpl(relation));
    byte[] bytes = ByteUtils.toBytes(reversedUnixTime);

    config.put(ORDER_PROPKEY, "desc");
    Formatter reversedSerde = new DateTime2LongFormatter(config);
    reversedSerde.cutAndSet(bytes, 0, bytes.length, f);
    Tuple t = f.pollTuple();
    assertEquals(iso_zoned_date, t.getAttribute(ATTR_NAME));
  }

  @Test
  public void testFilter() throws Exception {
    QuerySerializer serializer = mock(QuerySerializer.class);

    // when performing filtering, the formatter is deserialized from config.
    Map serializedConfig = serde.getProperties();
    Formatter deserializedSerde = new DateTime2LongFormatter(serializedConfig);

    GreaterthanOperator c = new GreaterthanOperator(ATTR_NAME, StringAttributeType.INSTANCE,
        iso_zoned_date);
    deserializedSerde.accept(serializer, Arrays.asList(c));
    verify(serializer)
        .write(null, new GreaterThanSegment(null, ByteUtils.toBytes(unixtime), false));
  }

  @Test
  public void testFilterReverseGreater() throws Exception {
    QuerySerializer serializer = mock(QuerySerializer.class);

    config.put(ORDER_PROPKEY, "desc");

    Map serializedConfig = new DateTime2LongFormatter(config).getProperties();
    Formatter deserializedSerde = new DateTime2LongFormatter(serializedConfig);

    GreaterthanOperator c = new GreaterthanOperator(ATTR_NAME, StringAttributeType.INSTANCE,
        iso_zoned_date);
    deserializedSerde.accept(serializer, Arrays.asList(c));
    verify(serializer)
        .write(null, new LessThanSegment(null, ByteUtils.toBytes(reversedUnixTime), false));
  }

  @Test
  public void testFilterReverseLesser() throws Exception {
    QuerySerializer serializer = mock(QuerySerializer.class);

    config.put(ORDER_PROPKEY, "desc");

    Map serializedConfig = new DateTime2LongFormatter(config).getProperties();
    Formatter deserializedSerde = new DateTime2LongFormatter(serializedConfig);

    LessthanOperator c = new LessthanOperator(ATTR_NAME, StringAttributeType.INSTANCE,
        iso_zoned_date);
    deserializedSerde.accept(serializer, Arrays.asList(c));
    verify(serializer
    ).write(null, new GreaterThanSegment(null, ByteUtils.toBytes(reversedUnixTime), false));
  }
}
