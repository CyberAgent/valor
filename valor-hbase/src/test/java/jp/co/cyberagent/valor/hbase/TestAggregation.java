package jp.co.cyberagent.valor.hbase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import jp.co.cyberagent.valor.hbase.repository.HBaseSchemaRepository;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.hbase.storage.SingleTableHBaseConnection;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.holder.FixedLengthHolder;
import jp.co.cyberagent.valor.sdk.holder.SuffixHolder;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.NotEqualOperator;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.zookeeper.repository.ZookeeperSchemaRepository;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAggregation {

  private static Logger LOG = LoggerFactory.getLogger(TestAggregation.class);

  private static final int NUM_RECORDS = 10;

  private static final String TABLE = "t";
  private static final String FAMILY = "f";

  private static final String SUFFIX_RELATION_ID = "suffix";
  private static final String FIX_LENGTH_RELATION_ID = "fixLength";
  private static final String PREFIX_RELATION_ID = "prefix";
  private static final String QUALIFIER_RELATION_ID = "qualifier";
  private static final String SCHEMA_ID = "v";

  private static final String KEY1 = "k1";
  private static final String KEY2 = "k2";
  private static final String KEY3 = "v3";

  private static HBaseTestingUtility utility;
  private static ValorContext context;
  private static ValorConnection conn;

  @SuppressWarnings("resource")
  @BeforeAll
  public static void setup() throws Exception {

    utility = new HBaseTestingUtility();
    utility.cleanupTestDir();
    utility.startMiniCluster(1);
    utility.createTable(Bytes.toBytes(HBaseSchemaRepository.SCHEMA_TABLE.defaultValue),
        Bytes.toBytes(HBaseSchemaRepository.SCHEMA_FAMILY.defaultValue));
    Configuration clusterConf = utility.getConfiguration();

    String zkQuorum = clusterConf.get(HConstants.ZOOKEEPER_QUORUM);
    String zkQuorumPort = clusterConf.get(HConstants.ZOOKEEPER_CLIENT_PORT);
    ValorConf conf = new ValorConfImpl();
    conf.set(StandardContextFactory.SCHEMA_REPOSITORY_CLASS.name, HBaseSchemaRepository.NAME);
    conf.set(ZookeeperSchemaRepository.SCHEMA_REPOS_ZKQUORUM.name, zkQuorum + ":" + zkQuorumPort);
    conf.set(HConstants.ZOOKEEPER_QUORUM, zkQuorum + ":" + zkQuorumPort);

    context = StandardContextFactory.create(conf);
    context.installPlugin(new HBasePlugin());

    byte[][] families = {Bytes.toBytes(FAMILY)};
    HTableDescriptor td = utility.createTableDescriptor(TABLE);
    td.addCoprocessor(SingleTableHBaseConnection.AGGR_COPROCESSOR);
    utility.createTable(td, families, clusterConf);

    Relation relation = ImmutableRelation.builder().relationId(SUFFIX_RELATION_ID)
        .addAttribute(KEY1, true, StringAttributeType.INSTANCE)
        .addAttribute(KEY2, true, StringAttributeType.INSTANCE)
        .addAttribute(KEY3, true, StringAttributeType.INSTANCE).build();

    SchemaDescriptor suffixSchema =
        ImmutableSchemaDescriptor.builder().isPrimary(true)
            .schemaId(SCHEMA_ID)
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(conf)
            .conf(conf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create(TABLE)))
            .addField(HBaseCell.ROWKEY, Arrays.asList(
                VintSizePrefixHolder.create(ConstantFormatter.create(SUFFIX_RELATION_ID)),
                SuffixHolder.create("\u0000", AttributeValueFormatter.create(KEY1)),
                SuffixHolder.create("\u0000", AttributeValueFormatter.create(KEY2)),
                AttributeValueFormatter.create(KEY3)))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create(FAMILY)))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(ConstantFormatter.create("")))
            .addField(HBaseCell.VALUE, Arrays.asList(ConstantFormatter.create("")))
            .build();

    SchemaDescriptor fixLengthSchema =
        ImmutableSchemaDescriptor.builder().isPrimary(true)
            .schemaId(SCHEMA_ID)
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(conf)
            .conf(conf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create(TABLE)))
            .addField(HBaseCell.ROWKEY, Arrays.asList(
                VintSizePrefixHolder.create(ConstantFormatter.create(FIX_LENGTH_RELATION_ID)),
                FixedLengthHolder.create(2, AttributeValueFormatter.create(KEY1)),
                FixedLengthHolder.create(2, AttributeValueFormatter.create(KEY2)),
                AttributeValueFormatter.create(KEY3)))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create(FAMILY)))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(ConstantFormatter.create("")))
            .addField(HBaseCell.VALUE, Arrays.asList(ConstantFormatter.create("")))
            .build();

    SchemaDescriptor prefixSchema =
        ImmutableSchemaDescriptor.builder().isPrimary(true)
            .schemaId(SCHEMA_ID)
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(conf)
            .conf(conf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create(TABLE)))
            .addField(HBaseCell.ROWKEY, Arrays.asList(
                VintSizePrefixHolder.create(ConstantFormatter.create(PREFIX_RELATION_ID)),
                VintSizePrefixHolder.create(AttributeValueFormatter.create(KEY1)),
                VintSizePrefixHolder.create(AttributeValueFormatter.create(KEY2)),
                AttributeValueFormatter.create(KEY3)))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create(FAMILY)))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(ConstantFormatter.create("")))
            .addField(HBaseCell.VALUE, Arrays.asList(ConstantFormatter.create("")))
            .build();

    SchemaDescriptor qualifierSchema =
        ImmutableSchemaDescriptor.builder().isPrimary(true)
            .schemaId(SCHEMA_ID)
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(conf)
            .conf(conf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create(TABLE)))
            .addField(HBaseCell.ROWKEY, Arrays.asList(
                VintSizePrefixHolder.create(ConstantFormatter.create(QUALIFIER_RELATION_ID)),
                VintSizePrefixHolder.create(AttributeValueFormatter.create(KEY1)),
                AttributeValueFormatter.create(KEY2)))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create(FAMILY)))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(AttributeValueFormatter.create(KEY3)))
            .addField(HBaseCell.VALUE, Arrays.asList(ConstantFormatter.create("")))
            .build();

    conn = ValorConnectionFactory.create(context);
    String[] relIds = {
        SUFFIX_RELATION_ID, FIX_LENGTH_RELATION_ID, PREFIX_RELATION_ID, QUALIFIER_RELATION_ID};
    SchemaDescriptor[] schemas = {suffixSchema, fixLengthSchema, prefixSchema, qualifierSchema};
    for (int i = 0; i < relIds.length; i++) {
      SchemaDescriptor schema = schemas[i];
      Relation rel = ImmutableRelation.copyOf(relation).withRelationId(relIds[i]);
      conn.createRelation(rel, false);
      conn.createSchema(rel.getRelationId(), schema, false);
      List<Tuple> tuples = IntStream.range(0, NUM_RECORDS)
          .mapToObj(j -> buildTuple(rel, "a", "b" + j, "v1"))
          .collect(Collectors.toList());
      conn.insert(rel.getRelationId(), tuples);
    }
  }

  @AfterAll
  public static void tearDown() throws Exception {
    conn.close();
    utility.shutdownMiniCluster();
  }

  private static class Fixture {
    public final String relationId;
    public final PredicativeExpression condition;
    public final Long result;

    Fixture(String relationId, PredicativeExpression condition, Long result) {
      this.relationId = relationId;
      this.condition = condition;
      this.result = result;
    }
  }

  @ParameterizedTest
  @MethodSource("getFixtures")
  public void testCount(Fixture fixture) throws Exception {
    assertCount(fixture.relationId, fixture.condition, fixture.result);
  }

  private static Fixture[] getFixtures() {
    AttributeNameExpression k1 = new AttributeNameExpression(KEY1, StringAttributeType.INSTANCE);
    AttributeNameExpression k2 = new AttributeNameExpression(KEY2, StringAttributeType.INSTANCE);
    PredicativeExpression key1Eq = new EqualOperator(k1, new ConstantExpression("a"));
    PredicativeExpression key1Neq = new NotEqualOperator(k1, new ConstantExpression("b"));
    PredicativeExpression key2lt = new LessthanOperator(k2, new ConstantExpression("b5"));
    PredicativeExpression key2gt = new GreaterthanOperator(k2, new ConstantExpression("b2"));
    PredicativeExpression key2lteq = new LessthanorequalOperator(k2, new ConstantExpression("b5"));
    PredicativeExpression key2gteq = new GreaterthanorequalOperator(k2, new ConstantExpression("b2"));

    return new Fixture[] {
        new Fixture(SUFFIX_RELATION_ID, key1Eq, (long)NUM_RECORDS),
        new Fixture(QUALIFIER_RELATION_ID, key1Eq, null),
        new Fixture(SUFFIX_RELATION_ID, key1Neq, null),
        new Fixture(SUFFIX_RELATION_ID, AndOperator.join(key1Eq, key2lt, key2gt), null),
        new Fixture(SUFFIX_RELATION_ID, AndOperator.join(key1Eq, key2lteq, key2gteq), 4L),
        new Fixture(PREFIX_RELATION_ID, AndOperator.join(key1Eq, key2lt, key2gt), null),
        new Fixture(PREFIX_RELATION_ID, AndOperator.join(key1Eq, key2lteq, key2gteq), null),
        new Fixture(FIX_LENGTH_RELATION_ID, AndOperator.join(key1Eq, key2lt, key2gt), 2l),
        new Fixture(FIX_LENGTH_RELATION_ID, AndOperator.join(key1Eq, key2lteq, key2gteq), 4l),
    };
  }

  private void assertCount(String relationId, PredicativeExpression condition, Long expected)
      throws ValorException {
    Optional<Long> v = conn.count(relationId, condition);
    if (expected == null) {
      assertFalse(v.isPresent());
    } else {
      assertEquals(expected, v.get());
    }
  }

  private static Tuple buildTuple(Relation relation, String k1, String k2, String k3) {
    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(KEY1, k1);
    tuple.setAttribute(KEY2, k2);
    tuple.setAttribute(KEY3, k3);
    return tuple;
  }
}
