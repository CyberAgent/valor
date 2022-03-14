package jp.co.cyberagent.valor.sdk.metadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import org.junit.jupiter.api.Test;


public class TestSchemaRepositoryCache {

  @Test
  public void testCacheEntryExpiresAfterTTL() throws InterruptedException{
    long ttlSec = 2;
    SchemaRepositoryCache relationCache = new SchemaRepositoryCache<String, Relation>(ttlSec);

    String relationId = "t";
    Relation relation = ImmutableRelation.builder().relationId(relationId).build();
    relationCache.put(relationId, relation);

    assertThat(relationCache.get(relationId), equalTo(relation));
    Thread.sleep((ttlSec + 1) * 1_000);
    assertThat(relationCache.get(relationId), equalTo(null));
  }
}
