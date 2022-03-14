package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestBetweenSegment {

  static class FilterFixture {
    final FilterSegment left;
    final FilterSegment right;
    final FilterSegment output;

    FilterFixture(FilterSegment left, FilterSegment right, FilterSegment output) {
      this.left = left;
      this.right = right;
      this.output = output;
    }
  }

  static FilterFixture[] fixtures = {
      new FilterFixture(
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08}),
          new BetweenSegment(null, true, new byte[] {0x01}, true, new byte[] {0x09}),
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08})),
      new FilterFixture(
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08}),
          new BetweenSegment(null, false, new byte[] {0x02}, false, new byte[] {0x08}),
          new BetweenSegment(null, false, new byte[] {0x02}, false, new byte[] {0x08})),
      new FilterFixture(
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08}),
          new BetweenSegment(null, false, new byte[] {0x03}, false, new byte[] {0x09}),
          new BetweenSegment(null, false, new byte[] {0x03}, true, new byte[] {0x08})),
      new FilterFixture(
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08}),
          new CompleteMatchSegment(null, new byte[] {0x03}),
          new CompleteMatchSegment(null, new byte[] {0x03})),
      new FilterFixture(
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08}),
          new CompleteMatchSegment(null, new byte[] {0x09}),
          FalseSegment.INSTANCE),
      new FilterFixture(
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08}),
          new LessThanSegment(null, new byte[] {0x07}, true),
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x07})),
      new FilterFixture(
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08}),
          new LessThanSegment(null, new byte[] {0x07}, true),
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x07})),
      new FilterFixture(
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08}),
          new GreaterThanSegment(null, new byte[] {0x01}, true),
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08})),
      new FilterFixture(
          new BetweenSegment(null, true, new byte[] {0x02}, true, new byte[] {0x08}),
          new GreaterThanSegment(null, new byte[] {0x03}, true),
          new BetweenSegment(null, true, new byte[] {0x03}, true, new byte[] {0x08}))
  };

  static FilterFixture[] getFixtures() {
    return fixtures;
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testMergeByAnd(FilterFixture fixture) {
    assertThat(fixture.left.mergeByAnd(fixture.right), equalTo(fixture.output));
    assertThat(fixture.right.mergeByAnd(fixture.left), equalTo(fixture.output));
  }

}
