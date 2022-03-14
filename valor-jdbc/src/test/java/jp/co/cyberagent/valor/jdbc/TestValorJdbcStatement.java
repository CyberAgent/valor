package jp.co.cyberagent.valor.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.Properties;
import jp.co.cyberagent.valor.sdk.SingletonStubPlugin;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestValorJdbcStatement {

  static final String URL = "jdbc:valor:";

  static final String REL_ID = "testTuple";
  static final String SCHEMA_ID = "testSchema";

  static final Properties conf = new Properties();

  @BeforeAll
  public static void init() throws Exception {
    conf.setProperty(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, SingletonStubPlugin.NAME);
    ValorConf vconf = new ValorPropertiesConfig(conf);
    ValorContext context = StandardContextFactory.create(vconf);
    Relation relation = ImmutableRelation.builder().relationId(REL_ID)
        .addAttribute("k1", true, StringAttributeType.INSTANCE)
        .addAttribute("k2", true, StringAttributeType.INSTANCE)
        .addAttribute("v", false, IntegerAttributeType.INSTANCE)
        .build();

    SchemaDescriptor schema = ImmutableSchemaDescriptor.builder()
        .schemaId(SCHEMA_ID)
        .storageClassName(SingletonStubPlugin.NAME)
        .storageConf(vconf)
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(AttributeValueFormatter.create("k1")))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(AttributeValueFormatter.create("k2")))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(AttributeValueFormatter.create("v")))
        .build();

    Tuple t1 = new TupleImpl(relation);
    t1.setAttribute("k1", "a");
    t1.setAttribute("k2", "b");
    t1.setAttribute("v", 100);

    Tuple t2 = new TupleImpl(relation);
    t2.setAttribute("k1", "a");
    t2.setAttribute("k2", "c");
    t2.setAttribute("v", 200);

    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.createRelation(relation, false);
      conn.createSchema(REL_ID, schema, false);
      conn.insert(REL_ID, t1, t2);
    }

    // unregister and update to set configuration of the mini cluster
    Class.forName(ValorDriver.class.getCanonicalName());
    Driver driver = DriverManager.getDriver(URL);
    DriverManager.deregisterDriver(driver);
    DriverManager.registerDriver(new ValorDriver());
  }

  @Test
  public void testSelect() throws Exception {
    try (Connection conn = DriverManager.getConnection(URL, conf)) {
      assertThat(conn, is(instanceOf(ValorJdbcConnection.class)));

      Statement stmt = conn.createStatement();
      assertThat(stmt, is(instanceOf(ValorJdbcStatement.class)));

      try (ResultSet rs = stmt.executeQuery(
          String.format("SELECT * FROM %s WHERE k1='a' and k2='b'", REL_ID))) {
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals(3, rsmd.getColumnCount());
        assertEquals("k1", rsmd.getColumnName(1));
        assertEquals("k2", rsmd.getColumnName(2));
        assertEquals("v", rsmd.getColumnName(3));
        assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
        assertEquals(Types.VARCHAR, rsmd.getColumnType(2));
        assertEquals(Types.INTEGER, rsmd.getColumnType(3));

        assertTrue(rs.next());
        assertEquals("b", rs.getString("k2"));
        assertEquals("b", rs.getObject("k2"));
        assertEquals("b", rs.getString(2));
        assertEquals("b", rs.getObject(2));
        assertEquals(100, rs.getInt("v"));
        assertEquals(100, rs.getObject("v"));
        assertEquals(100, rs.getInt(3));
        assertEquals(100, rs.getObject(3));
        assertFalse(rs.next());
      }

      try (ResultSet rs = stmt
          .executeQuery(String.format("SELECT v FROM %s WHERE k1='a'", REL_ID))) {
        ResultSetMetaData rsmd = rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals("v", rsmd.getColumnName(1));
        assertEquals(Types.INTEGER, rsmd.getColumnType(1));

        assertTrue(rs.next());
        assertEquals(100, rs.getInt("v"));
        assertTrue(rs.next());
        assertEquals(200, rs.getInt("v"));
        assertFalse(rs.next());
      }
    }
  }
}
