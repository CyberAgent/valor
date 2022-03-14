package jp.co.cyberagent.valor.spi.metadata;

import java.io.Closeable;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.IllegalSchemaException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;

public interface SchemaRepository extends Closeable {

  String DEFAULT_NAMESPACE = "default";

  void init(ValorContext context);

  ValorContext getContext();

  String getDefaultNameSpace();

  default Collection<String> listRelationIds() throws ValorException {
    return listRelationIds(getDefaultNameSpace());
  }

  Collection<String> listRelationIds(String namespace) throws ValorException;

  /**
   * get definition and schemaSets for a tuple
   *
   * @return null if tuple is not exist
   */
  default Relation findRelation(String relationId) throws ValorException {
    return findRelation(getDefaultNameSpace(), relationId);
  }

  Relation findRelation(String namespace, String relationId) throws ValorException;

  /**
   * @return the old definition if the metadata contains a relation for the
   * relation.relationId, otherwise null
   * @throws ValorException
   */
  default Relation createRelation(Relation relation, boolean overwrite) throws ValorException {
    return createRelation(getDefaultNameSpace(), relation, overwrite);
  }

  Relation createRelation(String namespace, Relation relation, boolean overwrite)
      throws ValorException;

  /**
   * @return an unregistered tuple definition if exists, otherwise null
   * @throws ValorException
   */
  default String dropRelation(String relationId) throws ValorException {
    return dropRelation(getDefaultNameSpace(), relationId);
  }

  String dropRelation(String namespace, String relationId) throws ValorException;

  /**
   * @return a list of schema defined for a given relation
   * @throws ValorException
   */
  default Collection<Schema> listSchemas(String relationId) throws ValorException {
    return listSchemas(getDefaultNameSpace(), relationId);
  }

  Collection<Schema> listSchemas(String namespace, String relationId) throws ValorException;

  /**
   * @return schema instance
   * @throws ValorException
   */
  default Schema findSchema(String relationId, String schemaId) throws ValorException {
    return findSchema(getDefaultNameSpace(), relationId, schemaId);
  }

  Schema findSchema(String namespace, String relationId, String schemaId) throws ValorException;

  /**
   * @return the old definition if already exists in the metadata, otherwise null
   * @throws ValorException
   */
  default Schema createSchema(String relationId, SchemaDescriptor schema, boolean overwrite)
      throws ValorException {
    return createSchema(getDefaultNameSpace(), relationId, schema, overwrite);
  }

  Schema createSchema(
      String namespace, String relationId, SchemaDescriptor schema, boolean overwrite)
      throws ValorException;

  /**
   * @return an unregistered schema if exists, otherwise null
   * @throws ValorException
   */
  default String dropSchema(String relationId, String schemaId) throws ValorException {
    return dropSchema(getDefaultNameSpace(), relationId, schemaId);
  }

  String dropSchema(String namespace, String relationId, String schemaId) throws ValorException;

  /**
   * change schema mode
   * @throws ValorException
   */
  default Schema.Mode setSchemaMode(String relationId, String schemaId, Schema.Mode mode)
      throws ValorException {
    return setSchemaMode(getDefaultNameSpace(), relationId, schemaId, mode);
  }


  Schema.Mode setSchemaMode(String namespace, String relationId, String schemaId, Schema.Mode mode)
      throws ValorException;

  default Schema toSchema(Relation relation, SchemaDescriptor descriptor) throws ValorException {
    Storage storage = getContext().createStorage(descriptor);
    return storage.buildSchema(relation, descriptor);
  }

  default Collection<Schema> replaceOrAddSchema(String relId, Schema schema,
                                                   Collection<Schema> existingSchemas)
      throws ValorException {
    IdMatcher idMatcher = new IdMatcher(schema.getSchemaId());
    existingSchemas =
        existingSchemas.stream().filter(idMatcher.negate()).collect(Collectors.toList());
    if (schema.isPrimary()) {
      Schema primary = existingSchemas.stream().filter(Schema::isPrimary).findAny().orElse(null);
      if (primary != null) {
        throw new IllegalSchemaException(relId, schema.getSchemaId(),
            "primary schema is already defined " + primary.getSchemaId());
      }
    }
    existingSchemas.add(schema);
    return existingSchemas;
  }

  Collection<String> getNamespaces();

  class IdMatcher implements Predicate<Schema> {

    private final String schemaId;

    public IdMatcher(String schemaId) {
      if (schemaId == null) {
        throw new IllegalArgumentException("schemaId is null");
      }
      this.schemaId = schemaId;
    }

    @Override
    public boolean test(Schema schema) {
      if (schema == null) {
        return false;
      }
      return schemaId.equals(schema.getSchemaId());
    }
  }
}
