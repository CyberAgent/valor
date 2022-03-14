package jp.co.cyberagent.valor.spi;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.QueryPlan;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.Query;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageMutation;

public interface ValorConnection extends Closeable {

  SchemaRepository getSchemaRepository();

  Collection<String> getNamespaces();

  ValorContext getContext();

  /**
   * create StorageConnection for a given schema.
   * Invokers of this method is responsible for closing the connection
   *
   * @return
   */
  StorageConnection createConnection(Schema schema) throws ValorException;

  /**
   * get relation
   *
   * @param relationId
   * @return a relation with the given relationId
   * @throws ValorException
   */
  default Relation findRelation(String relationId) throws ValorException {
    return getSchemaRepository().findRelation(relationId);
  }

  default Relation findRelation(String namespace, String relationId) throws ValorException {
    return getSchemaRepository().findRelation(namespace, relationId);
  }

  /**
   * list relationIds
   *
   * @return list of relationIds existing in the context
   * @throws ValorException
   */
  default Collection<String> listRelationsIds() throws ValorException {
    return getSchemaRepository().listRelationIds();
  }

  default Collection<String> listRelationsIds(String namespace) throws ValorException {
    return getSchemaRepository().listRelationIds(namespace);
  }

  /**
   * create or overwrite a relation definition
   *
   * @param relation  a new relation definition
   * @param overwrite flag whether overwriting an existing relation definition.
   * @return the old definition if exists, otherwise null
   * @throws ValorException
   */
  default Relation createRelation(Relation relation, boolean overwrite) throws ValorException {
    return getSchemaRepository().createRelation(relation, overwrite);
  }

  default Relation createRelation(String namespace, Relation relation, boolean overwrite)
      throws ValorException {
    return getSchemaRepository().createRelation(namespace, relation, overwrite);
  }

  /**
   * delete relation definition from the context metadata
   *
   * @param relationId relationId of a relation to be deleted
   * @throws ValorException
   */
  default void dropRelation(String relationId) throws ValorException {
    getSchemaRepository().dropRelation(relationId);
  }

  default void dropRelation(String namespace, String relationId) throws ValorException {
    getSchemaRepository().dropRelation(namespace, relationId);
  }

  /**
   * get schema
   *
   * @param relationId
   * @param schemaId
   * @return a schema of the relation with the given relationId, which identified by the given
   * schemaId
   * @throws ValorException
   */
  default Schema findSchema(String relationId, String schemaId) throws ValorException {
    return getSchemaRepository().findSchema(relationId, schemaId);
  }

  default Schema findSchema(String namespace, String relationid, String schemaId)
      throws ValorException {
    return getSchemaRepository().findSchema(namespace, relationid, schemaId);
  }

  /**
   * list schemaIds
   *
   * @param relationId
   * @return list of schemaIds defined for the relation with given relationId
   * @throws ValorException
   */
  default Collection<Schema> listSchemas(String relationId) throws ValorException {
    return getSchemaRepository().listSchemas(relationId);
  }

  default Collection<Schema> listSchemas(String namespace, String relationId)
      throws ValorException {
    return getSchemaRepository().listSchemas(namespace, relationId);
  }

  /**
   * create or overwrite a schema
   *
   * @param relationId relationId for which a new schema is defined
   * @param schema     a new schema definition
   * @param overwrite  flag whether overwriting an existing schema definition.
   * @return the old definition if exists, otherwise null
   * @throws ValorException
   */
  default Schema createSchema(String relationId, SchemaDescriptor schema, boolean overwrite)
      throws ValorException {
    return getSchemaRepository().createSchema(relationId, schema, overwrite);
  }

  default Schema createSchema(
      String namespace, String relationId, SchemaDescriptor schema, boolean overwrite)
      throws ValorException {
    return getSchemaRepository().createSchema(namespace, relationId, schema, overwrite);
  }

  /**
   * delete schema definition form the context metadata
   *
   * @param relationId relationId of which schema would be deleted
   * @param schemaId   schemaId of a schema to be deleted
   * @throws ValorException
   */
  default void dropSchema(String relationId, String schemaId) throws ValorException {
    getSchemaRepository().dropSchema(relationId, schemaId);
  }

  default void dropSchema(String namespace, String relationId, String schemaId)
      throws ValorException {
    getSchemaRepository().dropSchema(namespace, relationId, schemaId);
  }


  /**
   * an alias {@code select("default", relationId, null, selectItems, conditions)}
   *
   * @see ValorConnection#select(String, String, List, PredicativeExpression)
   */
  default List<Tuple> select(String relationId, List<String> selectItems,
                             PredicativeExpression conditions) throws IOException, ValorException {
    return select(null, relationId, selectItems, conditions);
  }

  /**
   * find tuples satisfying conditions from a relation.
   *
   * @param namespace
   * @param relationId  relationId of which tuples are read
   * @param selectItems attributes names to be projected
   * @param conditions  filter condition
   * @return tuples those satisfy condition
   * @throws IOException
   * @throws ValorException
   * @see Expression
   */
  List<Tuple> select(String namespace, String relationId, List<String> selectItems,
                     PredicativeExpression conditions) throws IOException, ValorException;

  /**
   * @deprecated replaced with {@link #scan(RelationScan)}
   * @param query
   * @return a list of tuples found by the query
   * @throws ValorException
   * @throws IOException
   */
  @Deprecated
  default List<Tuple> select(Query query) throws ValorException, IOException {
    return scan(query);
  }
  
  List<Tuple> scan(RelationScan scan) throws ValorException, IOException;

  /**
   * @deprecated replaced with {@link #compile(RelationScan)}
   * @param query
   * @return a query plan by the query
   * @throws ValorException
   * @throws IOException
   */
  @Deprecated
  QueryPlan plan(Query query) throws ValorException, IOException;

  TupleScanner compile(RelationScan scan) throws ValorException, IOException;

  // experimental
  default Optional<Long> count(String relationId, PredicativeExpression condition)
      throws ValorException {
    Relation relation = findRelation(relationId);
    return count(relation, condition);
  }

  default Optional<Long> count(Relation relation, PredicativeExpression condition)
      throws ValorException {
    Query query = Query.builder()
        .setRelationName(null, relation)
        .addItems(relation.getAttributes().stream()
            .filter(a -> a.isKey()).map(a -> new AttributeNameExpression(a.name(), a.type()))
            .collect(Collectors.toList()))
        .setCondition(condition)
        .build();
    try {
      QueryPlan plan = plan(query);
      return plan.count(this);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  /**
   * an alias to {@code insert(relationId, null, Arrays.asList(tuples))}
   *
   * @see ValorConnection#insert(String, String, Collection)
   */
  default void insert(String relationId, Tuple... tuple) throws IOException, ValorException {
    insert(null, relationId,  tuple);
  }

  /**
   * an alias to {@code insert(relationId, null, tuples)}
   *
   * @see ValorConnection#insert(String, String, Collection)
   */
  default void insert(String relationId, Collection<Tuple> tuples) throws IOException,
      ValorException {
    insert(null, relationId, tuples);
  }

  default void insert(String namespace, String relationId, Tuple... tuples)
      throws IOException, ValorException {
    insert(namespace, relationId, Arrays.asList(tuples));
  }

  /**
   * insert tuples into a relation
   *
   * @param namespace
   * @param relationId
   * @param tuples     tuples to be inserted
   * @throws IOException
   * @throws ValorException
   */
  void insert(String namespace, String relationId, Collection<Tuple> tuples)
      throws IOException, ValorException;

  /**
   * alias to {@code delete(relationId, null, condition, null)}
   *
   * @see ValorConnection#delete(String, String, PredicativeExpression, int)
   */
  default int delete(String relationId, PredicativeExpression condition)
      throws IOException, ValorException {
    return delete(null, relationId, condition, -1);
  }

  default int delete(String namespace, String relationId, PredicativeExpression condition)
      throws IOException, ValorException {
    return delete(null, relationId, condition, -1);
  }

  /**
   * delete tuples from a relation
   *
   * @param namespace
   * @param relationId
   * @param condition  filter condition for deleted tuples
   * @param limit      the maximum number of deleted tuples
   * @return number of records deleted
   * @throws IOException
   * @throws ValorException
   */
  int delete(String namespace, String relationId, PredicativeExpression condition, int limit)
      throws IOException, ValorException;




  /**
   * alias to {@code update("defult", relationId, null, newVals, condition)}
   *
   * @see ValorConnection#update(String, String, Map, PredicativeExpression)
   */
  default int update(String relationId, Map<String, Object> newVals,
                     PredicativeExpression condition) throws IOException, ValorException {
    return update(null, relationId, newVals, condition);
  }

  /**
   * update tuples satisfying conditions with new values.
   *
   * @param namespace
   * @param relationId
   * @param newVals    a map from attribute names to its new value
   * @param condition  filter condition to choose target tuples
   * @return number records updated
   * @throws IOException
   * @throws ValorException
   */
  int update(String namespace, String relationId, Map<String, Object> newVals,
             PredicativeExpression condition) throws IOException, ValorException;

  void submit(Schema schema, StorageMutation mutation) throws ValorException, IOException;

  default boolean isRelationAvailable(String relationId) throws ValorException, IOException {
    return isRelationAvailable(null, relationId);
  }

  boolean isRelationAvailable(String repoName, String relationId)
      throws ValorException, IOException;

}
