package jp.co.cyberagent.valor.hbase;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.hbase.storage.HBaseStorage;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.holder.SuffixHolder;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.Test;

public class TestHBaseStorage {

  private ValorConf conf = new ValorConfImpl();
  private Relation relation = ImmutableRelation.builder().relationId("test")
      .addAttribute("k1", true, StringAttributeType.INSTANCE)
      .addAttribute("k2", true, StringAttributeType.INSTANCE)
      .addAttribute("a1", false, StringAttributeType.INSTANCE)
      .addAttribute("a2", false, StringAttributeType.INSTANCE).build();

  @Test
  public void testContainsAttribute() throws Exception {
    SchemaDescriptor descriptor =
        ImmutableSchemaDescriptor.builder().schemaId("s1")
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create("tbl")))
            .addField(HBaseCell.ROWKEY, Arrays.asList(
                SuffixHolder.create("-", AttributeValueFormatter.create("k1")),
                AttributeValueFormatter.create("k2")))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create("fam")))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(AttributeValueFormatter.create("a1")))
            .addField(HBaseCell.VALUE, Arrays.asList(
                    SuffixHolder.create("-", AttributeValueFormatter.create("a2")),
                    AttributeValueFormatter.create("v1")))
            .build();
    HBaseStorage storage = new HBaseStorage(conf);
    Schema schema = storage.buildSchema(relation, descriptor);
    assertTrue(schema.containsAttribute("k1"));
    assertTrue(schema.containsAttribute("k2"));
    assertTrue(schema.containsAttribute("a1"));
    assertTrue(schema.containsAttribute("a2"));
    assertTrue(schema.containsAttribute("v1"));
    assertFalse(schema.containsAttribute("notIncluded"));
  }

  @Test
  public void testCanBePrimary() throws Exception {
    SchemaDescriptor descriptor =
        ImmutableSchemaDescriptor.builder().schemaId("s1")
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create("tbl")))
            .addField(HBaseCell.ROWKEY, Arrays.asList(
                SuffixHolder.create("-", AttributeValueFormatter.create("k1")),
                AttributeValueFormatter.create("k2")))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create("fam")))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(ConstantFormatter.create("qual")))
            .addField(HBaseCell.VALUE, Arrays.asList(AttributeValueFormatter.create("a1")))
            .build();

    HBaseStorage storage = new HBaseStorage(conf);
    Schema schema = storage.buildSchema(relation, descriptor);
    // "a2" is not included
    assertFalse(schema.conBePrimary(relation));

    descriptor =
        ImmutableSchemaDescriptor.builder().schemaId("s1")
            .storageClassName(HBaseStorage.class.getCanonicalName())
            .storageConf(conf)
            .addField(HBaseCell.TABLE, Arrays.asList(ConstantFormatter.create("tbl")))
            .addField(HBaseCell.ROWKEY, Arrays.asList(
                SuffixHolder.create("-", AttributeValueFormatter.create("k1")),
                AttributeValueFormatter.create("k2")))
            .addField(HBaseCell.FAMILY, Arrays.asList(ConstantFormatter.create("fam")))
            .addField(HBaseCell.QUALIFIER, Arrays.asList(AttributeValueFormatter.create("a1")))
            .addField(HBaseCell.VALUE, Arrays.asList(AttributeValueFormatter.create("a2")))
            .build();
    schema = storage.buildSchema(relation, descriptor);
    assertTrue(schema.conBePrimary(relation));
  }
}
