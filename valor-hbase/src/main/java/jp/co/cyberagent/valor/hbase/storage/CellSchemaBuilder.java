package jp.co.cyberagent.valor.hbase.storage;

import java.util.ArrayList;
import java.util.List;
import jp.co.cyberagent.valor.sdk.formatter.CumulativeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.DisassembleFormatter;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedCumulativeKeyValuesSchema;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedMonoKeyValueSchema;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedMultiKeyValuesSchema;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.IllegalSchemaException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;

public class CellSchemaBuilder {

  private Relation relation;
  private String storageClass;
  private ValorConf storageConf;
  private String schemaId;
  private FieldLayout rowkeyLayout;
  private FieldLayout familyLayout;
  private FieldLayout qualifierLayout;
  private FieldLayout timestampLayout;
  private FieldLayout valueLayout;

  private boolean isPrimary;
  private Schema.Mode mode = Schema.Mode.PUBLIC;
  private ValorConf schemaConf;

  private transient boolean multiRow = false;
  private transient boolean multiCell = false;
  private transient boolean cumulative = false;

  public CellSchemaBuilder setRelation(Relation relation) {
    this.relation = relation;
    return this;
  }

  public CellSchemaBuilder setStorageClass(String storageClass) {
    this.storageClass = storageClass;
    return this;
  }

  public CellSchemaBuilder setStorageConf(ValorConf storageConf) {
    this.storageConf = storageConf;
    return this;
  }

  public CellSchemaBuilder setSchemaId(String schemaId) {
    this.schemaId = schemaId;
    return this;
  }

  public CellSchemaBuilder setRowkeyLayout(FieldLayout rowkeyLayout) {
    this.rowkeyLayout = rowkeyLayout;
    return this;
  }

  public CellSchemaBuilder setFamilyLayout(FieldLayout familyLayout) {
    this.familyLayout = familyLayout;
    return this;
  }

  public CellSchemaBuilder setQualifierLayout(FieldLayout qualifierLayout) {
    this.qualifierLayout = qualifierLayout;
    return this;
  }

  public CellSchemaBuilder setTimestampLayout(FieldLayout timestampLayout) {
    this.timestampLayout = timestampLayout;
    return this;
  }

  public CellSchemaBuilder setValueLayout(FieldLayout valueLayout) {
    this.valueLayout = valueLayout;
    return this;
  }

  public CellSchemaBuilder setPrimary(boolean isPrimary) {
    this.isPrimary = isPrimary;
    return this;
  }

  public CellSchemaBuilder setMode(Schema.Mode mode) {
    this.mode = mode;
    return this;
  }

  public CellSchemaBuilder setSchemaConf(ValorConf schemaConf) {
    this.schemaConf = schemaConf;
    return this;
  }

  public Schema build(StorageConnectionFactory connectionFactory) throws ValorException {
    List<FieldLayout> parsedLayouts = new ArrayList<>();
    parsedLayouts.add(parseLaytout(HBaseCell.ROWKEY, rowkeyLayout));
    parsedLayouts.add(parseLaytout(HBaseCell.FAMILY, familyLayout));
    parsedLayouts.add(parseLaytout(HBaseCell.QUALIFIER, qualifierLayout));
    if (timestampLayout != null) {
      parsedLayouts.add(timestampLayout);
    }
    parsedLayouts.add(parseLaytout(HBaseCell.VALUE, valueLayout));

    SchemaDescriptor parsedDesciptor = ImmutableSchemaDescriptor.builder()
        .storageClassName(storageClass)
        .storageConf(storageConf)
        .schemaId(schemaId)
        .fields(parsedLayouts)
        .isPrimary(isPrimary)
        .mode(mode)
        .conf(schemaConf)
        .build();

    // FIXME : separate implementation of multiRow schema and multiCell schema
    if (multiRow || multiCell) {
      return cumulative ? new SortedCumulativeKeyValuesSchema(connectionFactory, relation,
          parsedDesciptor) :
          new SortedMultiKeyValuesSchema(connectionFactory, relation, parsedDesciptor);
    } else {
      return new SortedMonoKeyValueSchema(connectionFactory, relation, parsedDesciptor);
    }
  }

  private FieldLayout parseLaytout(String field, FieldLayout layout)
      throws IllegalSchemaException {
    if (!field.equals(layout.fieldName())) {
      throw new IllegalSchemaException(relation.getRelationId(), schemaId,
          field + " is expected but " + layout.fieldName());
    }

    boolean rowkey = HBaseCell.ROWKEY.equals(field);
    boolean key = rowkey || HBaseCell.FAMILY.equals(field) || HBaseCell.QUALIFIER.equals(field);
    for (Segment formatter : layout.formatters()) {
      Segment elm = formatter.getFormatter();
      boolean splitted = elm instanceof DisassembleFormatter;
      cumulative = cumulative || elm instanceof CumulativeValueFormatter;
      if (rowkey) {
        multiRow = multiRow || splitted;
      }
      if (key) {
        multiCell = multiCell || splitted;
        if (cumulative) {
          throw new IllegalSchemaException(relation.getRelationId(), schemaId,
              "cumulative element is not allowed in key");
        }
      }
    }
    return layout;
  }
}
