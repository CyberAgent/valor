package jp.co.cyberagent.valor.spi.schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Formatter implements Segment {
  public static final Logger LOG = LoggerFactory.getLogger(Formatter.class);

  @Override
  public Formatter getFormatter() {
    return this;
  }
}
