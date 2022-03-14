package jp.co.cyberagent.valor.sdk.schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.visitor.AttributeCollectVisitor;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.InvalidOperationException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.Record;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SchemaBase implements Schema {

  public static final Logger LOG = LoggerFactory.getLogger(SchemaBase.class);

  protected final StorageConnectionFactory connectionFactory;
  protected final Relation relation;
  protected final String schemaId;

  protected final List<FieldLayout> layouts;
  protected ValorConf conf;
  private boolean primary;
  private Schema.Mode mode;

  private transient Collection<String> containedAttributes;

  public SchemaBase(StorageConnectionFactory connectionFactory, Relation relation,
                    SchemaDescriptor schemaDescriptor) {
    this.connectionFactory = connectionFactory;
    this.relation = relation;
    this.schemaId = schemaDescriptor.getSchemaId();
    this.conf = schemaDescriptor.getConf();
    this.primary = schemaDescriptor.isPrimary();
    this.mode = schemaDescriptor.getMode();
    this.layouts = new ArrayList<>(schemaDescriptor.getFields().size());

    this.containedAttributes = new HashSet<>();
    Collection<String> attrs = relation.getAttributeNames();
    for (FieldLayout layout : schemaDescriptor.getFields()) {
      this.layouts.add(layout);
      for (Segment attrFormatter : layout.formatters()) {
        for (String a : attrs) {
          if (attrFormatter.containsAttribute(a)) {
            this.containedAttributes.add(a);
          }
        }
      }
    }
  }

  @Override
  public List<Record> serialize(Tuple tuple) throws ValorException {
    return getTupleSerializer().serailize(tuple, layouts);
  }

  @Override
  public SchemaScan buildScan(Collection<String> attributes, PredicativeExpression condition)
      throws ValorException {
    if (!validateScan(attributes, condition)) {
      return null;
    }
    OrOperator conjunctions = condition == null
        ? ConstantExpression.TRUE.getDnf() : condition.getDnf();
    SchemaScan scan = new SchemaScan(relation, this, condition);
    for (PredicativeExpression conjunction : conjunctions) {
      List<StorageScan> fragment = toScan(attributes, (AndOperator) conjunction);
      fragment.stream().forEach(scan::add);
    }
    return scan;
  }

  private boolean validateScan(Collection<String> attributes, PredicativeExpression condition) {
    if (primary) {
      return true;
    }
    // check all projection attributes included in this schema
    List<String> unsupportedAttributes = attributes.stream().filter(a -> !containsAttribute(a))
        .collect(Collectors.toList());
    if (!unsupportedAttributes.isEmpty()) {
      LOG.debug("required attribute is not included: {}", unsupportedAttributes);
      return false;
    }
    // check all filter attributes included in this schema
    if (condition != null) {
      AttributeCollectVisitor filterAttributeCollector = new AttributeCollectVisitor();
      condition.accept(filterAttributeCollector);
      Set<String> filterAttributes = filterAttributeCollector.getAttrributes();
      if (filterAttributes.stream().filter(a -> !containsAttribute(a)).findAny().isPresent()) {
        return false;
      }
    }
    return true;
  }

  protected List<StorageScan> toScan(Collection<String> attributes, AndOperator conjunction)
      throws ValorException {
    Set<String> required = conf.containsKey(SELECT_REQUIRED)
        ? Arrays.stream(conf.get(SELECT_REQUIRED).split(",")).collect(Collectors.toSet())
        : Collections.EMPTY_SET;

    List<PrimitivePredicate> predicates = new ArrayList<>(conjunction.getOperands().size());
    for (PredicativeExpression p : conjunction) {
      if (!(p instanceof PrimitivePredicate)) {
        throw new IllegalArgumentException(conjunction + " is not normalized");
      }
      predicates.add((PrimitivePredicate) p);
      if (p instanceof EqualOperator) {
        String attr = ((EqualOperator) p).getAttributeIfUnaryPredicate();
        if (p != null) {
          required.remove(attr);
        }
      }
    }
    if (!required.isEmpty()) {
      throw new InvalidOperationException(
          conjunction + " does not include " + Arrays.asList(required.toArray()));
    }

    return getQuerySerializer().serailize(attributes, predicates, layouts);
  }

  @Override
  public String getRelationId() {
    return relation == null ? null : relation.getRelationId();
  }

  @Override
  public String getSchemaId() {
    return this.schemaId;
  }

  @Override
  public Schema.Mode getMode() {
    return this.mode;
  }

  @Override
  public void setMode(Schema.Mode mode) {
    this.mode = mode;
  }

  @Override
  public Storage getStorage() {
    return connectionFactory.getStorage();
  }

  @Override
  public StorageConnectionFactory getConnectionFactory() {
    return connectionFactory;
  }

  @Override
  public List<String> getFields() {
    return layouts.stream().map(FieldLayout::getFieldName).collect(Collectors.toList());
  }

  @Override
  public FieldLayout getLayout(String fieldName) {
    if (fieldName == null) {
      return null;
    }
    return this.layouts.stream().filter(f -> fieldName.equals(f.getFieldName())).findFirst()
        .orElse(null);
  }

  @Override
  public ValorConf getConf() {
    return conf;
  }

  @Override
  public void setConf(ValorConf conf) {
    this.conf = conf;
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("schemaId: " + this.schemaId);
    buf.append(System.getProperty("line.separator"));
    buf.append("primary:  " + (this.isPrimary() ? "YES" : "NO"));
    buf.append(System.getProperty("line.separator"));
    buf.append("mode:     " + this.mode.name());
    buf.append(System.getProperty("line.separator"));
    for (FieldLayout formatter : layouts) {
      StringJoiner joiner = new StringJoiner(":");
      for (Segment e : formatter.getFormatters()) {
        joiner.add(e.toString());
      }
      buf.append(formatter.getFieldName() + ":    " + joiner.toString());
      buf.append(System.getProperty("line.separator"));
    }
    for (Map.Entry<String, String> opt : this.conf) {
      buf.append(String.format("%s=%s", opt.getKey(), opt.getValue()));
    }
    return buf.toString();
  }

  @Deprecated
  @Override
  public boolean containsAttribute(String attr) {
    for (FieldLayout layout : layouts) {
      for (Segment attrFormatter : layout.getFormatters()) {
        if (attrFormatter.containsAttribute(attr)) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public Collection<String> getContainedAttributes() {
    return containedAttributes;
  }

  @Override
  public boolean conBePrimary(Relation relation) {
    for (String attr : relation.getAttributeNames()) {
      if (!containsAttribute(attr)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isPrimary() {
    return this.primary;
  }

  @Override
  public void setPrimary(boolean primary) {
    this.primary = primary;
  }
}
