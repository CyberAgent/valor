package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class TestCompleteMatchSegment {

  @Test
  public void testMergeWithNotMatch() {
    CompleteMatchSegment base = new CompleteMatchSegment(null, new byte[] {0x09});
    assertThat(base.mergeByAnd(new NotMatchSegment(null, new byte[] {0x09})),
        is(instanceOf(FalseSegment.class)));
    assertThat(base.mergeByAnd(new NotMatchSegment(null, new byte[] {0x01})), equalTo(base));
  }

  @Test
  public void testMergeWithLessthan() {
    CompleteMatchSegment base = new CompleteMatchSegment(null, new byte[] {0x09});
    assertThat(base.mergeByAnd(new LessThanSegment(null, new byte[] {0x09}, false)),
        is(instanceOf(FalseSegment.class)));
    assertThat(base.mergeByAnd(new LessThanSegment(null, new byte[] {0x09}, true)), equalTo(base));
    assertThat(base.mergeByAnd(new LessThanSegment(null, new byte[] {0x10}, false)), equalTo(base));
    assertThat(base.mergeByAnd(new LessThanSegment(null, new byte[] {0x10}, true)), equalTo(base));
    assertThat(base.mergeByAnd(new LessThanSegment(null, new byte[] {0x08}, true)),
        is(instanceOf(FalseSegment.class)));
    assertThat(base.mergeByAnd(new LessThanSegment(null, new byte[] {0x08}, false)),
        is(instanceOf(FalseSegment.class)));
  }

  @Test
  public void testMergeWithGreaterthan() {
    CompleteMatchSegment base = new CompleteMatchSegment(null, new byte[] {0x09});
    assertThat(base.mergeByAnd(new GreaterThanSegment(null, new byte[] {0x09}, false)),
        is(instanceOf(FalseSegment.class)));
    assertThat(base.mergeByAnd(new GreaterThanSegment(null, new byte[] {0x09}, true)), equalTo(base));
    assertThat(base.mergeByAnd(new GreaterThanSegment(null, new byte[] {0x10}, false)),
        is(instanceOf(FalseSegment.class)));
    assertThat(base.mergeByAnd(new GreaterThanSegment(null, new byte[] {0x10}, true)),
        is(instanceOf(FalseSegment.class)));
    assertThat(base.mergeByAnd(new GreaterThanSegment(null, new byte[] {0x08}, true)), equalTo(base));
    assertThat(base.mergeByAnd(new GreaterThanSegment(null, new byte[] {0x08}, false)), equalTo(base));
  }
}
