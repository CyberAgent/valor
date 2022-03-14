package jp.co.cyberagent.valor.spi.plan.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.LogicalPlanVisitor;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;

public class RelationSource implements FromClause {

  private String namespace;

  private Relation relation;

  private Schema schema;

  public RelationSource(Relation relation) {
    this(null, relation, null);
  }

  public RelationSource(String namespace, Relation relation) {
    this(namespace, relation, null);
  }

  public RelationSource(Relation relation, Schema schema) {
    this(null, relation, schema);
  }

  public RelationSource(String namespace, Relation relation, Schema schema) {
    this.namespace = namespace;
    this.relation = relation;
    this.schema = schema;
  }

  public Collection<Schema> listSchemas(SchemaRepository repository)
      throws ValorException {
    if (schema == null) {
      return repository.listSchemas(namespace, relation.getRelationId());
    } else {
      return Arrays.asList(schema);
    }
  }

  public String getNamespace() {
    return this.namespace;
  }

  public Relation getRelation() {
    return relation;
  }

  public Schema getSchema() {
    return schema;
  }


  @Override
  public List<Relation.Attribute> getAttributes() {
    return relation.getAttributes();
  }

  @Override
  public void accept(LogicalPlanVisitor visitor) {
    visitor.visit(this);
    visitor.leave(this);;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RelationSource)) {
      return false;
    }
    RelationSource that = (RelationSource) o;
    return Objects.equals(namespace, that.namespace)
        && Objects.equals(relation, that.relation) && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, relation, schema);
  }

  @Override
  public String toString() {
    String relationId = relation.getRelationId();
    String name = namespace == null ? relationId :  String.format("%s.%s", namespace, relationId);
    return "FROM " + name;
  }

}
