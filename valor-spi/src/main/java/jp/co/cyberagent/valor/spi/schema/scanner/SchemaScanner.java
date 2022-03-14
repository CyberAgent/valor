package jp.co.cyberagent.valor.spi.schema.scanner;

import java.io.Closeable;
import java.io.IOException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Tuple;

public interface SchemaScanner extends Closeable {

  Tuple next() throws IOException, ValorException;
}
