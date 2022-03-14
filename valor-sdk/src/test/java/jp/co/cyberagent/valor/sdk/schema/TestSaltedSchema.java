package jp.co.cyberagent.valor.sdk.schema;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.SingletonStubPlugin;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.Murmur3SaltFormatter;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestSaltedSchema {

  @Test
  public void test() throws Exception {
    final String REL_ID = "r";
    final String SCHEMA_ID = "s1";
    final String KEY_ATTR = "k";
    final String VAL_ATTR = "v";

    ValorConf conf = new ValorConfImpl();
    conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, SingletonStubPlugin.NAME);
    ValorContext context = StandardContextFactory.create(conf);

    Relation relation = ImmutableRelation.builder()
        .relationId(REL_ID)
        .addAttribute(KEY_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(VAL_ATTR, false, StringAttributeType.INSTANCE)
        .build();

    Map<String, Object> saltConfig = new HashMap<>();
    saltConfig.put(Murmur3SaltFormatter.RANGE_PROPKEY, 2);
    saltConfig.put(Murmur3SaltFormatter.ATTRIBUTES_NAME_PROPKEY, Arrays.asList(KEY_ATTR));

    SchemaDescriptor ws1 = ImmutableSchemaDescriptor.builder()
        .schemaId(SCHEMA_ID)
        .storageClassName(SingletonStubPlugin.NAME)
        .storageConf(conf)
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(
            new Murmur3SaltFormatter(saltConfig),
            new AttributeValueFormatter(KEY_ATTR))
        )
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(new ConstantFormatter(REL_ID)))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(new AttributeValueFormatter(VAL_ATTR)))
        .build();

    Tuple t = new TupleImpl(relation);
    t.setAttribute(KEY_ATTR, "k1");
    t.setAttribute(VAL_ATTR, "v1");

    final PrimitivePredicate filter
        = new EqualOperator(KEY_ATTR, StringAttributeType.INSTANCE, "k1");
    final byte[] startKey = ByteUtils.toBytes("k1");
    final byte[] stopKey = ByteUtils.toBytes("k2");
    final byte[] salt0 = new byte[]{0x00};
    final byte[] salt1 = new byte[]{0x01};

    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.createRelation(relation, true);
      conn.createSchema(relation.getRelationId(), ws1, true);

      Schema s = conn.findSchema(REL_ID, SCHEMA_ID);
      QuerySerializer qs = s.getQuerySerializer();
      List<StorageScan> scans = qs.serailize(
          relation.getAttributeNames(), Arrays.asList(filter), ws1.getFields());
      assertThat(scans, hasSize(2));

      StorageScan scan = scans.get(0);
      assertArrayEquals(ByteUtils.add(salt0, startKey), scan.getStart(EmbeddedEKVStorage.KEY));
      assertArrayEquals(ByteUtils.add(salt0, stopKey), scan.getStop(EmbeddedEKVStorage.KEY));
      scan = scans.get(1);
      assertArrayEquals(ByteUtils.add(salt1, startKey), scan.getStart(EmbeddedEKVStorage.KEY));
      assertArrayEquals(ByteUtils.add(salt1, stopKey), scan.getStop(EmbeddedEKVStorage.KEY));

      conn.insert(REL_ID, t);
      List<Tuple> result = conn.select(REL_ID, Arrays.asList(KEY_ATTR, VAL_ATTR), filter);
      assertThat(result, hasSize(1));
    }
  }
}
