package jp.co.cyberagent.valor.spi.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;


import java.util.Arrays;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.GreaterThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.LessThanSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.RegexpMatchSegment;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestFieldComparator {

  @Test
  public void testMerge() {
    //public static FieldComparator buildTuple(Operator op, byte[] prefix, byte[] start, byte[]
    // stop, byte[] regexp){
    FieldComparator f1 = FieldComparator.build(FieldComparator.Operator.BETWEEN, ByteUtils.toBytes(
        "prefix"), ByteUtils.toBytes("a"), ByteUtils.toBytes("c"), null);
    FieldComparator f2 = FieldComparator.build(FieldComparator.Operator.BETWEEN, ByteUtils.toBytes(
        "prefix"), ByteUtils.toBytes("b"), ByteUtils.toBytes("d"), null);
    FieldComparator merged = f1.mergeByOr(f2);
    assertThat(merged.getOperator(), equalTo(FieldComparator.Operator.BETWEEN));
    assertThat(ByteUtils.toString(merged.getStart()), equalTo("a"));
    assertThat(ByteUtils.toString(merged.getStop()), equalTo("d"));
  }

  public static class Fixture {
    FieldComparator.Operator operator;
    byte[] start;
    byte[] stop;
    FilterSegment[] segments;

    Fixture(
        FieldComparator.Operator operator, byte[] start, byte[] stop, FilterSegment... segments) {
      this.operator = operator;
      this.start = start;
      this.stop = stop;
      this.segments = segments;
    }
  }

  static Fixture[] fixtures = {
      new Fixture(FieldComparator.Operator.BETWEEN,
          ByteUtils.toBytes("prefix"), ByteUtils.toBytes("prefiy"),
          new CompleteMatchSegment(null, ByteUtils.toBytes("prefix")),
          new RegexpMatchSegment(null, ByteUtils.toBytes(".*"), true)),
      new Fixture(FieldComparator.Operator.BETWEEN,
          ByteUtils.toBytes("prefix"), ByteUtils.toBytes("prefiy"),
          new CompleteMatchSegment(null, ByteUtils.toBytes("prefix")),
          TrueSegment.INSTANCE),
      new Fixture(FieldComparator.Operator.GREATER,
          ByteUtils.toBytes("prefix"), null,
          new GreaterThanSegment(null, ByteUtils.toBytes("prefix"), true),
          new LessThanSegment(null, ByteUtils.toBytes("suffix"), false)),
      new Fixture(FieldComparator.Operator.LESS,
          ByteUtils.toBytes(""), ByteUtils.toBytes("prefiy"),
          new LessThanSegment(null, ByteUtils.toBytes("prefix"), true),
          new GreaterThanSegment(null, ByteUtils.toBytes("suffix"), false)),
      new Fixture(FieldComparator.Operator.BETWEEN,
          ByteUtils.toBytes("prefixsuffix"), ByteUtils.toBytes("prefiy"),
          new CompleteMatchSegment(null, ByteUtils.toBytes("prefix")),
          new GreaterThanSegment(null, ByteUtils.toBytes("suffix"), false)),
      new Fixture(FieldComparator.Operator.BETWEEN,
          ByteUtils.toBytes("prefix"), ByteUtils.toBytes("prefixsuffix"),
          new CompleteMatchSegment(null, ByteUtils.toBytes("prefix")),
          new LessThanSegment(null, ByteUtils.toBytes("suffix"), false))
  };

  static Fixture[] getFixtures() {
    return fixtures;
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testAppend(Fixture fixture) {
    FieldComparator c = new FieldComparator();
    Arrays.stream(fixture.segments).forEach(s -> c.append(s));
    assertThat(c.getOperator(), equalTo(fixture.operator));
    assertArrayEquals(fixture.start, c.getStart());
    assertArrayEquals(fixture.stop, c.getStop());
  }

}
