package jp.co.cyberagent.valor.spi.relation;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.DefaultSchemaHandler;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaHandler;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true, depluralize = true)
public interface Relation {

  boolean IS_NULLABLE_IN_DEFAULT = false;

  @JsonIgnore
  String getRelationId();

  List<Attribute> getAttributes();

  @Value.Default
  default SchemaHandler getSchemaHandler() {
    return new DefaultSchemaHandler();
  }

  List<SchemaDescriptor> getSchemas();

  @Value.Check
  default Relation init() {
    getSchemaHandler().init(this);
    return this;
  }

  default ScanPlan plan(ValorConnection conn,
                        Collection<Schema> schemas,
                        List<ProjectionItem> items,
                        PredicativeExpression condition,
                        Planner planner) throws ValorException {
    return getSchemaHandler().plan(conn, schemas, items, condition, planner);
  }

  default TupleScanner scanner(
      ValorConnection conn,
      Collection<Schema> schemas,
      List<ProjectionItem> items,
      PredicativeExpression condition,
      Planner planner) throws ValorException {
    return getSchemaHandler().scanner(conn, schemas, items, condition, planner);
  }

  default void insert(ValorConnection conn, Collection<Schema> schemas, Collection<Tuple> tuples)
      throws ValorException, IOException {
    getSchemaHandler().insert(conn, schemas, tuples);
  }

  default int delete(ValorConnection conn,
                     Collection<Schema> schemas, TupleScanner scanner, Integer limit)
      throws ValorException, IOException {
    return getSchemaHandler().delete(conn, schemas, scanner, limit);
  }

  default int update(ValorConnection conn,
                     Collection<Schema> schemas, TupleScanner scanner, Map<String, Object> newVals)
      throws ValorException, IOException {
    return getSchemaHandler().update(conn, schemas, scanner, newVals);
  }

  @Value.Lazy
  default List<String> getAttributeNames() {
    return getAttributes().stream().map(Attribute::name).collect(Collectors.toList());
  }

  @Value.Lazy
  default List<String> getKeyAttributeNames() {
    return getAttributes().stream().filter(Attribute::isKey).map(Attribute::name)
        .collect(Collectors.toList());
  }

  @Value.Lazy
  default Map<String, Attribute> getAttributeMapByName() {
    return getAttributes().stream().collect(Collectors.toMap(a -> a.name(), a -> a));
  }

  default Attribute getAttribute(String attr) {
    return getAttributeMapByName().get(attr);
  }

  default AttributeType getAttributeType(String attr) {
    Attribute a = getAttribute(attr);
    return a == null ? null : a.type();
  }

  default boolean isKey(String attr) {
    Attribute a = getAttribute(attr);
    return a == null ? false : a.isKey();
  }

  // mvoe to elsewhare
  enum OperationType {
    WRITE, READ
  }

  @Value.Immutable
  @Value.Style(
      allMandatoryParameters = true, visibility = Value.Style.ImplementationVisibility.PUBLIC)
  interface Attribute {
    String name();

    boolean isKey();

    AttributeType type();

    @Value.Default
    default boolean isNullable() {
      return IS_NULLABLE_IN_DEFAULT;
    }
  }

}
