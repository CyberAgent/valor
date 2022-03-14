package jp.co.cyberagent.valor.sdk.storage.fs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;


import java.util.List;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.metadata.FileSchemaRepository;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.Test;

public class TestFsStorage {


  @Test
  public void testSelect() throws Exception {
    ValorConf conf = new ValorConfImpl();
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, FileSchemaRepository.NAME);
    conf.set(FileSchemaRepository.SCHEMA_REPOS_BASEDIR.name, "src/test/resources/fs/schema");
    ValorContext context = StandardContextFactory.create(conf);
    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      Relation rel = conn.findRelation("fstest");
      List<Tuple> tuples = conn.select("fstest", rel.getAttributeNames(),
          new EqualOperator("k", StringAttributeType.INSTANCE, "sample"));
      assertThat(tuples, hasSize(2));
      Tuple t = tuples.get(0);
      assertThat(t.getAttribute("v"), equalTo("aaa"));
      t = tuples.get(1);
      assertThat(t.getAttribute("v"), equalTo("bbb"));
    }

  }
}
