package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;

public class TrueSegment implements FilterSegment {

  public static final TrueSegment INSTANCE = new TrueSegment();

  private TrueSegment() {
  }

  @Override
  public Type type() {
    return Type.TRUE;
  }

  @Override
  public FilterSegment mergeByAnd(FilterSegment otherFragment) {
    return otherFragment;
  }

  @Override
  public boolean evaluate(byte[] value) {
    return true;
  }
}
