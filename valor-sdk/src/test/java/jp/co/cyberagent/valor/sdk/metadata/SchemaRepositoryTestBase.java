package jp.co.cyberagent.valor.sdk.metadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.util.Arrays;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.IllegalRelationException;
import jp.co.cyberagent.valor.spi.exception.IllegalSchemaException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public abstract class SchemaRepositoryTestBase {

  protected static Relation relation;
  protected static SchemaDescriptor schemaDef;
  protected SchemaRepository repo;

  protected static SchemaRepository setInitialSchema(SchemaRepository repo) throws Exception {
    relation = ImmutableRelation.builder().relationId("t")
        .addAttribute("attr", true, StringAttributeType.INSTANCE)
        .build();
    schemaDef =
        ImmutableSchemaDescriptor.builder().schemaId("s")
            .isPrimary(true)
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(new ValorConfImpl())
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(ConstantFormatter.create("s")))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(AttributeValueFormatter.create("attr")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList())
            .build();

    repo.createRelation(relation, true);
    repo.createSchema("t", schemaDef, true);

    return repo;
  }

  // @Test flaky ignore tentatively
  public void testRedefineTupleOrSchema() throws Exception {
    try {
      repo.createRelation(relation, false);
      fail("unexpected tuple defintition overwrite");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalRelationException);
    }
    Relation prevTuple = repo.createRelation(relation, true);
    assertEquals(relation, prevTuple, relation + "\n" + prevTuple);

    try {
      repo.createSchema(relation.getRelationId(), schemaDef, false);
      fail("unexpected schema defintition overwrite");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalSchemaException);
    }
    Schema prevSchema = repo.createSchema(relation.getRelationId(), schemaDef, true);
    Thread.sleep(3000);
    assertEquals(prevSchema.getSchemaId(), schemaDef.getSchemaId());
    assertEquals(prevSchema.getMode(), schemaDef.getMode());
  }

  @Test
  public void testHasTupleOrSchema() throws IOException, ValorException {
    assertTrue(repo.listRelationIds().contains("t"));
    assertFalse(repo.listRelationIds().contains("s"));
    assertTrue(repo.listSchemas("t").stream().anyMatch(new SchemaRepository.IdMatcher("s")));
  }

  @Test
  public void testDropTupleOrSchema() throws Exception {

    repo.createRelation(ImmutableRelation.builder().relationId("r").addAttribute("attr", true,
        StringAttributeType.INSTANCE).build(), true);

    SchemaDescriptor schemaToBeRemoved =
        ImmutableSchemaDescriptor.builder().schemaId("r")
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(new ValorConfImpl())
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(ConstantFormatter.create("r")))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(AttributeValueFormatter.create("attr")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList()).build();
    repo.createSchema("t", schemaToBeRemoved, true);
    // override to test deletion of updated schema
    repo.createSchema("t", schemaToBeRemoved, true);

    repo.createSchema("r",
        ImmutableSchemaDescriptor.builder().schemaId("s")
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(new ValorConfImpl())
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(ConstantFormatter.create("s")))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(AttributeValueFormatter.create("attr")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList())
            .build(), true);

    assertTrue(repo.listRelationIds().contains("r"));
    assertTrue(repo.listSchemas("t").stream().anyMatch(new SchemaRepository.IdMatcher("r")));
    repo.dropRelation("r");
    repo.dropSchema("t", "r");
    assertFalse(repo.listRelationIds().contains("r"));
    assertExistenceInPersistentStore(false, "r");
    assertFalse(repo.listSchemas("t").stream().anyMatch(new SchemaRepository.IdMatcher("r")));
    assertExisitenceInPersistentStore(false, "t", "r");
    assertTrue(repo.listRelationIds().contains("t"));
    assertExistenceInPersistentStore(true, "t");
    assertFalse(repo.listRelationIds().contains("s"));
    assertExistenceInPersistentStore(false, "s");
    assertTrue(repo.listSchemas("t").stream().anyMatch(new SchemaRepository.IdMatcher("s")));
    assertExisitenceInPersistentStore(true, "t", "s");
  }

  protected abstract void assertExistenceInPersistentStore(Boolean shouldExist, String relId)
      throws Exception;

  protected abstract void assertExisitenceInPersistentStore(Boolean shouldExist, String relId,
                                                            String schemaId) throws Exception;

  protected void dump() throws Exception {
  }

  @Test
  public void testDuplicatePrimarySchema() throws Exception {
    Assertions.assertThrows(IllegalSchemaException.class, () -> {
      repo.createSchema("t",
          ImmutableSchemaDescriptor.builder().schemaId("dup").isPrimary(true)
              .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
              .storageConf(new ValorConfImpl())
              .addField(EmbeddedEKVStorage.KEY, Arrays.asList(ConstantFormatter.create("dup")))
              .addField(EmbeddedEKVStorage.COL,
                  Arrays.asList(AttributeValueFormatter.create("attr")))
              .addField(EmbeddedEKVStorage.VAL, Arrays.asList())
              .build(), true);
    });
  }

  @Test
  public void testOverridePrimarySchema() throws Exception {
    repo.createSchema("t",
        ImmutableSchemaDescriptor.builder().schemaId("s").isPrimary(true)
            .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
            .storageConf(new ValorConfImpl())
            .addField(EmbeddedEKVStorage.KEY, Arrays.asList(ConstantFormatter.create("s")))
            .addField(EmbeddedEKVStorage.COL, Arrays.asList(AttributeValueFormatter.create("attr")))
            .addField(EmbeddedEKVStorage.VAL, Arrays.asList())
            .build(), true);
  }

  @Test
  public void testChangeSchemaMode() throws Exception {
    Schema entry = repo.findSchema(relation.getRelationId(), schemaDef.getSchemaId());
    assertEquals(Schema.Mode.PUBLIC, entry.getMode());
    repo.setSchemaMode(relation.getRelationId(), schemaDef.getSchemaId(), Schema.Mode.READ_ONLY);
    Thread.sleep(1000);
    entry = repo.findSchema(relation.getRelationId(), schemaDef.getSchemaId());
    assertEquals(Schema.Mode.READ_ONLY, entry.getMode());
    repo.setSchemaMode(relation.getRelationId(), schemaDef.getSchemaId(), Schema.Mode.PUBLIC);
    Thread.sleep(1000);
    entry = repo.findSchema(relation.getRelationId(), schemaDef.getSchemaId());
    assertEquals(Schema.Mode.PUBLIC, entry.getMode());
  }
}
