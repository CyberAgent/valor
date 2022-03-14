package jp.co.cyberagent.valor.trino;

import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import java.util.List;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;

/**
 *
 */
public class ValorRecordSet implements RecordSet {

  private final TupleScanner runner;
  private final List<Type> columnTypes;
  private final List<ValorColumnHandle> columns;
  private final ClassLoader classLoader;

  public ValorRecordSet(
      TupleScanner runner, List<? extends ColumnHandle> columns, ClassLoader classLoader) {
    this.runner = runner;
    this.columns = columns.stream().map(ValorColumnHandle.class::cast).collect(Collectors.toList());
    this.columnTypes =
        this.columns.stream().map(ValorColumnHandle::getColumnType).collect(Collectors.toList());
    this.classLoader = classLoader;
  }

  @Override
  public List<Type> getColumnTypes() {
    return columnTypes;
  }

  @Override
  public RecordCursor cursor() {
    return new ValorRecordCursor(columns, runner, classLoader);
  }
}
