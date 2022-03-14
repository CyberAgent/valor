package jp.co.cyberagent.valor.spi.schema.scanner.filter;

import jp.co.cyberagent.valor.spi.schema.scanner.FilterSegment;

public class FalseSegment implements FilterSegment {

  public static final FilterSegment INSTANCE = new FalseSegment();

  private FalseSegment(){
  }

  @Override
  public Type type() {
    return Type.FALSE;
  }

  @Override
  public FilterSegment mergeByAnd(FilterSegment otherFragment) {
    return this;
  }

  @Override
  public boolean evaluate(byte[] value) {
    return false;
  }

}
