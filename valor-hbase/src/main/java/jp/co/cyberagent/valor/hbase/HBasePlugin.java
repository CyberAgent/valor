package jp.co.cyberagent.valor.hbase;

import java.util.Arrays;
import java.util.Collections;
import jp.co.cyberagent.valor.hbase.formatter.MapTtlValueFormatter;
import jp.co.cyberagent.valor.hbase.formatter.TtlFormatter;
import jp.co.cyberagent.valor.hbase.optimize.HBaseOptimizer;
import jp.co.cyberagent.valor.hbase.repository.HBaseSchemaRepository;
import jp.co.cyberagent.valor.hbase.schema.HBaseDefaultSchemaHandler;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.hbase.storage.snapshot.HBaseSnapshotStorage;
import jp.co.cyberagent.valor.spi.Plugin;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.spi.optimize.OptimizerFactory;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.SchemaHandlerFactory;
import jp.co.cyberagent.valor.spi.storage.StorageFactory;

public class HBasePlugin implements Plugin {
  @Override
  public Iterable<SchemaRepositoryFactory> getSchemaRepositoryFactories() {
    return Arrays.asList(new HBaseSchemaRepository.Factory());
  }

  @Override
  public Iterable<StorageFactory> getStorageFactories() {
    return Arrays.asList(
        new HBaseStorage.Factory(),
        new HBaseSnapshotStorage.Factory()
    );
  }

  @Override
  public Iterable<FormatterFactory> getFormatterFactories() {
    return Arrays.asList(new TtlFormatter.Factory(),
                         new MapTtlValueFormatter.Factory());
  }

  @Override
  public Iterable<OptimizerFactory> getOptimizerFactories() {
    return Collections.singleton(new HBaseOptimizer.Factory());
  }

  @Override
  public Iterable<SchemaHandlerFactory> getSchemaHandlerFactories() {
    return Collections.singleton(new HBaseDefaultSchemaHandler.Factory());
  }
}
