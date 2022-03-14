package jp.co.cyberagent.valor.sdk.storage.kv;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.formatter.MultiAttributeNamesFormatter;
import jp.co.cyberagent.valor.sdk.holder.VintSizePrefixHolder;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.Planner;
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

public class EmbeddedKVDefaultSchemaHandler extends SchemaHandlerBase {

  public static final String NAME = "embeddedKvDefaultSchema";

  public static final String ATTACH_RELID_AS_PREFIX_KEY
      = "valor.embeddedKv.attach.relationIdAsPrefix";

  public static SchemaDescriptor buildDefaultSchema(
      Map<String, Object> config, Relation relation) {
    ValorConf storageConf = new ValorConfImpl();
    boolean attachRelId = false;
    for (Map.Entry p : config.entrySet()) {
      if (ATTACH_RELID_AS_PREFIX_KEY.equals(p.getKey())) {
        attachRelId = Boolean.valueOf(p.getValue().toString());
      } else {
        storageConf.set(p.getKey().toString(), p.getValue().toString());
      }
    }
    String[] keyAttrs = relation.getAttributeNames()
        .toArray(new String[relation.getAttributeNames().size()]);
    List<Segment> keySegments = new ArrayList<>(keyAttrs.length);
    if (attachRelId) {
      keySegments.add(VintSizePrefixHolder.create(
          ConstantFormatter.create(relation.getRelationId())));
    }
    for (String keyAttr : relation.getAttributeNames()) {
      keySegments.add(VintSizePrefixHolder.create(new AttributeValueFormatter(keyAttr)));
    }

    return ImmutableSchemaDescriptor.builder().isPrimary(true)
        .schemaId(UUID.randomUUID().toString())
        .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
        .storageConf(storageConf)
        .conf(storageConf)
        .addField(EmbeddedEKVStorage.KEY, keySegments)
        .addField(EmbeddedEKVStorage.COL,
            Arrays.asList(MultiAttributeNamesFormatter.create(keyAttrs)))
        .addField(EmbeddedEKVStorage.VAL,
            Arrays.asList(MultiAttributeNamesFormatter.create(keyAttrs)))
        .build();
  }

  private transient SchemaDescriptor schemaDescriptor;

  private final AtomicReference<Collection<Schema>> schemas = new AtomicReference<>();

  @Override
  public SchemaHandler init(Relation relation) {
    super.init(relation);
    schemaDescriptor = buildDefaultSchema(conf, relation);
    return this;
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
      EmbeddedKVDefaultSchemaHandler handler = new EmbeddedKVDefaultSchemaHandler();
      handler.configure(config);
      return handler;
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends SchemaHandler> getProvidedClass() {
      return EmbeddedKVDefaultSchemaHandler.class;
    }
  }


}
