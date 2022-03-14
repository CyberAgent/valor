package jp.co.cyberagent.valor.zookeeper;

import java.util.Arrays;
import jp.co.cyberagent.valor.spi.Plugin;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.zookeeper.repository.ZookeeperSchemaRepository;

public class ZookeeperPlugin implements Plugin {

  @Override
  public Iterable<SchemaRepositoryFactory> getSchemaRepositoryFactories() {
    return Arrays.asList(new ZookeeperSchemaRepository.Factory());
  }
}
