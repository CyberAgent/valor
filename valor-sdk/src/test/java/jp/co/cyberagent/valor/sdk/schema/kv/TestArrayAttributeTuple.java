package jp.co.cyberagent.valor.sdk.schema.kv;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsEqual.equalTo;

import java.util.Arrays;
import java.util.List;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.plan.function.ArrayContainsFunction;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.plan.model.NotOperator;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.ArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScanner;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;
import org.junit.jupiter.api.Test;

public class TestArrayAttributeTuple {

  private static final String KEY_ATTR = "keyAttr";
  private static final String VAL_ATTR = "valAttr";
  private final Relation relation;
  private Schema schema;
  private Storage storage;

  public TestArrayAttributeTuple() throws Exception {
    ValorConf conf = new ValorConfImpl();

    // define map<string, string> type
    ArrayAttributeType arrayType = new ArrayAttributeType();
    arrayType.addGenericElementType(StringAttributeType.INSTANCE);

    relation = ImmutableRelation.builder().relationId("ar")
        .addAttribute(KEY_ATTR, true, StringAttributeType.INSTANCE)
        .addAttribute(VAL_ATTR, false, arrayType).build();

    SchemaDescriptor schemaDescriptor =
        ImmutableSchemaDescriptor.builder().schemaId("test")
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(EmbeddedEKVStorage.KEY,
                Arrays.asList(AttributeValueFormatter.create(KEY_ATTR)))
            .addField(EmbeddedEKVStorage.COL,
                Arrays.asList(ConstantFormatter.create("a")))
            .addField(EmbeddedEKVStorage.VAL,
                Arrays.asList(AttributeValueFormatter.create(VAL_ATTR)))
            .build();
    storage = new EmbeddedEKVStorage(conf);
    schema = storage.buildSchema(relation, schemaDescriptor);
  }

  @SuppressWarnings(value = "unchecked")
  @Test
  public void test() throws Exception {

    Tuple tuple = new TupleImpl(relation);
    tuple.setAttribute(KEY_ATTR, "k1");
    tuple.setAttribute(VAL_ATTR, Arrays.asList("a", "b", "c"));

    try (StorageConnection conn = schema.getConnectionFactory().connect()) {
      StorageMutation mutation = schema.buildInsertMutation(tuple);
      mutation.execute(conn);
      ArrayContainsFunction posFilter = new ArrayContainsFunction(VAL_ATTR, "b");
      SchemaScan scan = schema.buildScan(relation.getAttributeNames(), posFilter);
      try (SchemaScanner schemaScanner = schema.getScanner(scan, conn)) {
        Tuple scanned = schemaScanner.next();
        assertThat(scanned.getAttribute(KEY_ATTR), equalTo("k1"));
        List<Object> v = (List<Object>) scanned.getAttribute(VAL_ATTR);
        assertThat(v, hasSize(3));
        assertThat(v, hasItem("a"));
        assertThat(v, hasItem("b"));
        assertThat(v, hasItem("c"));
      }

      ArrayContainsFunction negFilter = new ArrayContainsFunction(VAL_ATTR, "x");
      scan = schema.buildScan(relation.getAttributeNames(), negFilter);
      try (SchemaScanner schemaScanner = schema.getScanner(scan, conn)) {
        Tuple scanned = schemaScanner.next();
        assertThat(scanned, is(nullValue()));
      }

      scan = schema.buildScan(relation.getAttributeNames(), new NotOperator(negFilter));
      try (SchemaScanner schemaScanner = schema.getScanner(scan, conn)) {
        Tuple scanned = schemaScanner.next();
        assertThat(scanned.getAttribute(KEY_ATTR), equalTo("k1"));
        List<Object> v = (List<Object>) scanned.getAttribute(VAL_ATTR);
        assertThat(v, hasSize(3));
        assertThat(v, hasItem("a"));
        assertThat(v, hasItem("b"));
        assertThat(v, hasItem("c"));
      }
    }
  }
}
