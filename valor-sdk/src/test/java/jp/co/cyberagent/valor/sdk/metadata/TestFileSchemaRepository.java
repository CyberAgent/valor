package jp.co.cyberagent.valor.sdk.metadata;

import java.io.File;
import java.io.IOException;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

public class TestFileSchemaRepository extends SchemaRepositoryTestBase {

  @SuppressWarnings("resource")
  @BeforeEach
  public void initRepo() throws Exception {
    ValorConf conf = new ValorConfImpl();
    ValorContext context = StandardContextFactory.create(conf);

    File tmp = File.createTempFile("tmp", null);
    File baseDir = new File(tmp.getParent(), String.valueOf(System.currentTimeMillis()));
    baseDir.mkdir();
    conf.set(FileSchemaRepository.SCHEMA_REPOS_BASEDIR.name, baseDir.getPath());

    this.repo = new FileSchemaRepository(conf);
    this.repo.init(context);
    setInitialSchema(repo);
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
