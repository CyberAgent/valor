package jp.co.cyberagent.valor.sdk.formatter;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.Arrays;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;

public class FilterFixture {
  public final PrimitivePredicate predicate;
  public final FilterSegment filter;

  public FilterFixture(PrimitivePredicate predicate, FilterSegment filter) {
    this.predicate = predicate;
    this.filter = filter;
  }

  public static void testFilterFixture(Formatter formatter, FilterFixture fixture) throws ValorException {
    QuerySerializer serializer = mock(QuerySerializer.class);
    formatter.accept(serializer, Arrays.asList(fixture.predicate));
    verify(serializer).write(null, fixture.filter);
  }
}
