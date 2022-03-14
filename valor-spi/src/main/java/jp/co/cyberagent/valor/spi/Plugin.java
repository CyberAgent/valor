package jp.co.cyberagent.valor.spi;

import java.util.Collections;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.spi.optimize.OptimizerFactory;
import jp.co.cyberagent.valor.spi.plan.PlannerFactory;
import jp.co.cyberagent.valor.spi.plan.model.Udf;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.HolderFactory;
import jp.co.cyberagent.valor.spi.schema.SchemaHandlerFactory;
import jp.co.cyberagent.valor.spi.storage.StorageFactory;

public interface Plugin {

  default Iterable<SchemaRepositoryFactory> getSchemaRepositoryFactories() {
    return Collections.emptyList();
  }

  default Iterable<StorageFactory> getStorageFactories() {
    return Collections.emptyList();
  }

  default Iterable<FormatterFactory> getFormatterFactories() {
    return Collections.emptyList();
  }

  default Iterable<HolderFactory> getHolderFactories() {
    return Collections.emptyList();
  }

  default Iterable<PlannerFactory> getPlannerFactories() {
    return Collections.emptyList();
  }

  default Iterable<PluggableFactory<Udf, Void>> getUdfFactories() {
    return Collections.emptyList();
  }

  default Iterable<OptimizerFactory> getOptimizerFactories() {
    return Collections.emptyList();
  }

  default Iterable<SchemaHandlerFactory> getSchemaHandlerFactories() {
    return Collections.emptyList();
  }
}
