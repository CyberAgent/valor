package jp.co.cyberagent.valor.trino;

import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.resultBuilder;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.StandaloneQueryRunner;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.SingletonStubPlugin;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestValorTrino {

  static final String SCHEMA_DEF = "schema  = "
            + "'{"
            + "      \"relationId\" : \"%s\","
            + "      \"schemaId\" : \"s\","
            + "      \"isPrimary\" : true,"
            + "      \"storage\" : {"
            + "         \"class\" : \"jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage\", "
            + "         \"conf\" : {}"
            + "      },"
            + "      \"fields\" : ["
            + "        {\"name\" : \"key\", \"format\" : ["
            + "          {\"type\" : \"size\", "
            + "           \"decoratee\" : {\"type\": \"const\", \"props\" : {\"value\":\"%s\"}}},"
            + "          {\"type\" : \"size\", "
            + "           \"decoratee\" : {\"type\": \"attr\", \"props\" : {\"attr\":\"k1\"}}}"
            + "        ]},"
            + "        {\"name\" : \"col\", \"format\" : [{\"type\": \"const\", \"props\" : {\"value\":\"c\"}}]},"
            + "        {\"name\" : \"val\", \"format\" : [{\"type\": \"attr\", \"props\" : {\"attr\":\"v\"}}]}"
            + "      ]"
            + "}'";

  private static final Session SESSION = testSessionBuilder()
          .setCatalog("valor")
          .setSchema("default").build();

  private StandaloneQueryRunner queryRunner;

  private Map<String,String> conf = new HashMap() {
    {
      put(StandardContextFactory.SCHEMA_REPOSITORY_CLASS.name, SingletonStubPlugin.NAME);
    }
  };

  @SuppressWarnings("unchecked")
  @BeforeEach
  public void setup() throws Exception {
    queryRunner = new StandaloneQueryRunner(SESSION);
    queryRunner.installPlugin(new ValorPlugin());
    // workaround of service loader is not working in unit test
    queryRunner.createCatalog("valor", "valor", conf);
  }

  @AfterEach
  public void tearDown() throws Exception {
    queryRunner.close();
  }

  @Test
  public void testSelectAndDelete() throws Exception {
    final String TABLE = "testSelect";
    queryRunner.execute("DROP TABLE IF EXISTS " + TABLE);
    queryRunner.execute("create table " + TABLE + " (k1 varchar, v bigint) "
            + "WITH (" + String.format(SCHEMA_DEF, TABLE, TABLE) + ")");
    queryRunner.execute("Describe " + TABLE);


    queryRunner.execute("insert into " + TABLE + " values ('first', 100)");
    queryRunner.execute("insert into " + TABLE + " values (null, 200)");
    MaterializedResult result = queryRunner.execute("SELECT count(1) from " + TABLE + " where k1 = 'first'");
    assertThat(result.getRowCount(), equalTo(1));
    assertThat(result.getMaterializedRows().get(0).getField(0), equalTo(1l));


    result = queryRunner.execute("SELECT count(1) from " + TABLE + " where k1 is null");
    assertThat(result.getRowCount(), equalTo(1));
    assertThat(result.getMaterializedRows().get(0).getField(0), equalTo(1l));

    result = queryRunner.execute("SELECT count(1) from " + TABLE + " where k1 is not null");
    assertThat(result.getRowCount(), equalTo(1));
    assertThat(result.getMaterializedRows().get(0).getField(0), equalTo(1L));

    result = queryRunner.execute("SELECT count(1) from " + TABLE);
    assertThat(result.getRowCount(), equalTo(1));
    assertThat(result.getMaterializedRows().get(0).getField(0), equalTo(2l));

    result = queryRunner.execute("SELECT * from " + TABLE + " ORDER BY v ASC");
    assertThat(result.getRowCount(), equalTo(2));
    assertThat(result.getMaterializedRows().get(0).getField(0), equalTo("first"));
    assertThat(result.getMaterializedRows().get(0).getField(1), equalTo(100L));
    assertThat(result.getMaterializedRows().get(1).getField(0), is(nullValue()));
    assertThat(result.getMaterializedRows().get(1).getField(1), equalTo(200L));

    queryRunner.execute(
        "insert into " + TABLE + " SELECT 'copied', v * 3 FROM " + TABLE + " WHERE k1 = 'first'");
    result = queryRunner.execute("SELECT * from " + TABLE + " ORDER BY v ASC");
    assertThat(result.getRowCount(), equalTo(3));
    assertThat(result.getMaterializedRows().get(0).getField(0), equalTo("first"));
    assertThat(result.getMaterializedRows().get(0).getField(1), equalTo(100L));
    assertThat(result.getMaterializedRows().get(1).getField(0), is(nullValue()));
    assertThat(result.getMaterializedRows().get(1).getField(1), equalTo(200L));
    assertThat(result.getMaterializedRows().get(2).getField(0), equalTo("copied"));
    assertThat(result.getMaterializedRows().get(2).getField(1), equalTo(300L));

  }

  @Test
  public void testSelectArrayColumn(){
    final String TABLE = "testSelectArrayColumn";
    queryRunner.execute("DROP TABLE IF EXISTS " + TABLE);
    queryRunner.execute("CREATE TABLE " + TABLE + "(k1 varchar, v ARRAY<VARCHAR>)"
        + "WITH (" + String.format(SCHEMA_DEF, TABLE, TABLE) + ")");

    queryRunner.execute("INSERT INTO " + TABLE + " VALUES ('first', ARRAY['second', 'third'])");
    MaterializedResult result = queryRunner.execute("SELECT * FROM " + TABLE + " where k1 = 'first'");
    MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, result.getTypes())
        .row("first", Arrays.asList("second", "third"))
        .build();
    assertThat(result, equalTo(expected));
  }

  @Test
  public void testSelectMapColumn(){
    final String TABLE = "testSelectMapColumn";
    queryRunner.execute("DROP TABLE IF EXISTS " + TABLE);
    queryRunner.execute("CREATE TABLE " + TABLE + "(k1 varchar, v MAP<VARCHAR, VARCHAR>)"
            + "WITH (" + String.format(SCHEMA_DEF, TABLE, TABLE) + ")");

    queryRunner.execute("INSERT INTO " + TABLE + " VALUES ('first',MAP(ARRAY['second'], ARRAY['third']))");
    MaterializedResult result = queryRunner.execute("SELECT * FROM " + TABLE + " where k1 = 'first'");
    MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, VARCHAR, mapType(VARCHAR, VARCHAR))
            .row("first", new HashMap<String, String>(){{ put("second", "third"); }})
            .build();
    assertThat(result, equalTo(expected));
  }

  @Test
  public void testSelectNestedArrayMapColumn(){
    final String TABLE = "testSelectNestedArrayMapColumn";
    queryRunner.execute("DROP TABLE IF EXISTS " + TABLE);
    queryRunner.execute("CREATE TABLE " + TABLE + "(k1 varchar, v ARRAY<MAP<VARCHAR, VARCHAR>>)"
            + "WITH (" + String.format(SCHEMA_DEF, TABLE, TABLE) + ")");

    queryRunner.execute(
        "INSERT INTO " + TABLE + " VALUES ('first',ARRAY[MAP(ARRAY['second'], ARRAY['third'])])");
    MaterializedResult result = queryRunner.execute("SELECT * FROM " + TABLE + " where k1 = 'first'");
    List<Type> types = result.getTypes();
    MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, types.get(0), types.get(1))
            .row("first", ImmutableList.of(new HashMap<String, String>(){{ put("second", "third"); }}))
            .build();
    assertThat(result, equalTo(expected));
  }

  @Test
  public void testCreateIntegerColumn() {
    final String TABLE = "createIntegerColumn";
    queryRunner.execute("DROP TABLE IF EXISTS " + TABLE);
    queryRunner.execute("create table " + TABLE + " (k1 integer, v integer) "
        + "WITH (" + String.format(SCHEMA_DEF, TABLE, TABLE) + ")");

    queryRunner.execute("insert into " + TABLE + " values (100, 1)");
    MaterializedResult result = queryRunner.execute("SELECT * from " + TABLE + " where k1 > 50");
    MaterializedResult expected = resultBuilder(SESSION, INTEGER, INTEGER)
                    .row(100, 1)
                    .build();
    assertThat(result, equalTo(expected));
    result = queryRunner.execute("SELECT * from " + TABLE + " where v = 1");
    assertThat(result, equalTo(expected));
  }

  @Test
  public void testDescribeTable() {
    final String TABLE = "describeTable";
    queryRunner.execute("DROP TABLE IF EXISTS " + TABLE);
    queryRunner.execute("create table " + TABLE + " (k1 integer, v integer) "
        + "WITH (" + String.format(SCHEMA_DEF, TABLE, TABLE) + ")");
    MaterializedResult described = queryRunner.execute("DESCRIBE " + TABLE);
    MaterializedResult expected = resultBuilder(SESSION, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
            .row("k1", "integer", "", "")
            .row("v", "integer", "", "")
            .build();
    assertThat(described, equalTo(expected));
  }

  @Test
  public void testCaseInsensitive() throws Exception {
    final String TABLE = "caseInsensitive";
    Relation rel = ImmutableRelation.builder()
        .relationId(TABLE)
        .addAttribute("k", true, StringAttributeType.INSTANCE)
        .addAttribute("v", false, StringAttributeType.INSTANCE)
        .build();
    ValorContext context = StandardContextFactory.create(conf);
    SchemaRepository repo = context.createRepository(context.getConf());
    repo.createRelation(rel, false);

    MaterializedResult described = queryRunner.execute("DESCRIBE " + TABLE);
    MaterializedResult expected = resultBuilder(SESSION, VARCHAR, VARCHAR, VARCHAR, VARCHAR)
        .row("k", "varchar", "", "")
        .row("v", "varchar", "", "")
        .build();
    assertThat(described, equalTo(expected));
  }
}
