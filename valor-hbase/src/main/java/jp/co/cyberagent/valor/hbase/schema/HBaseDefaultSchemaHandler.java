package jp.co.cyberagent.valor.hbase.schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeNamesFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeValuesFormatter;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaHandler;
import jp.co.cyberagent.valor.spi.schema.SchemaHandlerBase;
import jp.co.cyberagent.valor.spi.schema.SchemaHandlerFactory;
import jp.co.cyberagent.valor.spi.schema.Segment;

public class HBaseDefaultSchemaHandler extends SchemaHandlerBase {

  public static final String NAME = "hbaseDefaultSchema";

  public static final String ATTACH_RELID_AS_PREFIX_KEY = "valor.hbase.attach.relationIdAsPrefix";

  private transient SchemaDescriptor schemaDescriptor;

  private final AtomicReference<Collection<Schema>> schemas = new AtomicReference<>();

  @Override
  public SchemaHandler init(Relation relation) {
    super.init(relation);
    schemaDescriptor = buildSchema(relation, this.conf);
    return this;
  }

  public static SchemaDescriptor buildSchema(Relation relation, Map<String, Object> conf) {
    boolean attachRelId = false;
    ValorConf valorConf = new ValorConfImpl();
    for (Map.Entry p : conf.entrySet()) {
      if (ATTACH_RELID_AS_PREFIX_KEY.equals(p.getKey())) {
        attachRelId = Boolean.valueOf(p.getValue().toString());
      } else if (!HBaseStorage.FAMILY_PARAM_KEY.equals(p.getKey())) {
        valorConf.set(p.getKey().toString(), p.getValue().toString());
      }
    }

    String[] keyAttrs = relation.getKeyAttributeNames()
        .toArray(new String[relation.getKeyAttributeNames().size()]);
    Segment[] keySegments;
    int idx = 0;
    if (attachRelId) {
      keySegments = new Segment[keyAttrs.length + 1];
      keySegments[idx++]
          = VintSizePrefixHolder.create(ConstantFormatter.create(relation.getRelationId()));
    } else {
      keySegments = new Segment[keyAttrs.length];
    }

    for (String keyAttr : keyAttrs) {
      keySegments[idx++] = VintSizePrefixHolder.create(new AttributeValueFormatter(keyAttr));
    }

    return ImmutableSchemaDescriptor.builder().isPrimary(true)
            .schemaId(UUID.randomUUID().toString())
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(valorConf)
            .conf(new ValorConfImpl())
            .addField(HBaseCell.ROWKEY, Arrays.asList(keySegments))
            .addField(HBaseCell.FAMILY, Arrays.asList(
                ConstantFormatter.create((String) conf.get(HBaseStorage.FAMILY_PARAM_KEY))))
            .addField(
                HBaseCell.QUALIFIER, Arrays.asList(MultiAttributeNamesFormatter.create(keyAttrs)))
            .addField(
                HBaseCell.VALUE, Arrays.asList(MultiAttributeValuesFormatter.create(keyAttrs)))
            .build();
  }

  @Override
  public ScanPlan plan(ValorConnection conn, Collection<Schema> schemas, List<ProjectionItem> items,
                       PredicativeExpression condition, Planner planner) throws ValorException {
    Collection<Schema> ss = getSchema(conn.getContext(), schemas);
    return planner.plan(items, condition, ss);
  }

  @Override
  public TupleScanner scanner(ValorConnection conn, Collection<Schema> schemas,
                              List<ProjectionItem> items, PredicativeExpression condition,
                              Planner planner) throws ValorException {
    Collection<Schema> ss = getSchema(conn.getContext(), schemas);
    return super.scanner(conn, ss, items, condition, planner);
  }

  @Override
  public void insert(ValorConnection conn, Collection<Schema> schemas, Collection<Tuple> tuples)
      throws ValorException, IOException {
    Collection<Schema> ss = getSchema(conn.getContext(), schemas);
    super.insert(conn, ss, tuples);
  }

  private Collection<Schema> getSchema(ValorContext context, Collection<Schema> arguments) {
    if (!arguments.isEmpty()) {
      return arguments;
    }
    Collection<Schema> schemas = this.schemas.get();
    if (schemas != null) {
      return schemas;
    }
    try {
      schemas = Arrays.asList(context.buildSchmea(relation, schemaDescriptor));
      boolean updated = this.schemas.compareAndSet(null, schemas);
      return updated ? schemas : this.schemas.get();
    } catch (ValorException e) {
      throw new IllegalArgumentException(e);
    }

  }

  @Override
  public String getName() {
    return NAME;
  }

  public static class Factory implements SchemaHandlerFactory {

    @Override
    public SchemaHandler create(Map config) {
      HBaseDefaultSchemaHandler handler = new HBaseDefaultSchemaHandler();
      handler.configure(config);
      return handler;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends SchemaHandler> getProvidedClass() {
      return HBaseDefaultSchemaHandler.class;
    }
  }


}
