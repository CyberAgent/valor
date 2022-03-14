package jp.co.cyberagent.valor.sdk.metadata;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.SingletonStubPlugin;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.spi.ValorContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class TestInMemorySchemaRepository extends SchemaRepositoryTestBase {

  @SuppressWarnings("resource")
  @BeforeEach
  public void initRepo() throws Exception {
    Map<String, String> conf = new HashMap<String, String>(){
      {
        put("valor.schemarepository.class", SingletonStubPlugin.NAME);
      }
    };

    ValorContext context = StandardContextFactory.create(conf);
    context.installPlugin(new SingletonStubPlugin());
    this.repo = context.createRepository(context.getConf());
    repo.init(context);
    SchemaRepositoryTestBase.setInitialSchema(repo);
  }

  @AfterEach
  public void closeRepo() throws IOException {
    this.repo.close();
  }

  @Override
  protected void assertExistenceInPersistentStore(Boolean shouldExist, String relId)
      throws IOException {
  }

  @Override
  protected void assertExisitenceInPersistentStore(Boolean shouldExist, String relId,
                                                   String schemaId) throws IOException {
  }
}
