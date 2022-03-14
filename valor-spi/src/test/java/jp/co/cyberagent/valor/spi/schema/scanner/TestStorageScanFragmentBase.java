package jp.co.cyberagent.valor.spi.schema.scanner;

import org.junit.jupiter.api.Test;

public class TestStorageScanFragmentBase {

  @Test
  public void testScanOptimziation() throws Exception {
    /** TODO
     Relation relation =
     new RelationBuilder().setRelationId("test")
     .setAttribute("k1", true, StringAttributeType.INSTANCE)
     .setAttribute("a1", false, StringAttributeType.INSTANCE).buildTuple();
     Schema schemaDef =
     new SchemaDescriptor().setRelation(relation).setPrimary(false).setSchemaId("schema")
     .setTableFormatters(ConstantFormatter.create("tbl"))
     .setRowFormatters(AttributeValueFormatter.create("k1"))
     .setFamilyFormatters(ConstantFormatter.create("fam"))
     .setQualifierFormatters(ConstantFormatter.create("qaul"))
     .setValueFormatters(AttributeValueFormatter.create("a1")).buildTuple();
     PriorityQueue<Scan> ss = new PriorityQueue<Scan>(1, DefaultSchemaScan.STARTROW_COMPARATOR);
     ss.add(new Scan(Bytes.toBytes("a1"), Bytes.toBytes("a2")));
     ss.add(new Scan(Bytes.toBytes("c1"), Bytes.toBytes("c2")));
     ss.add(new Scan(Bytes.toBytes("b1"), Bytes.toBytes("b2")));
     SchemaScanBase<Tuple> scan = new StubSchemaScan(schemaDef, new Dnf());
     PriorityQueue<Scan> optss = invoke(scan, "optimize", ss);
     assertEquals(3, optss.size());
     Scan s = optss.poll();
     assertArrayEquals(Bytes.toBytes("a1"), s.getStartRow());
     assertArrayEquals(Bytes.toBytes("a2"), s.getStopRow());
     s = optss.poll();
     assertArrayEquals(Bytes.toBytes("b1"), s.getStartRow());
     assertArrayEquals(Bytes.toBytes("b2"), s.getStopRow());
     s = optss.poll();
     assertArrayEquals(Bytes.toBytes("c1"), s.getStartRow());
     assertArrayEquals(Bytes.toBytes("c2"), s.getStopRow());
     */
  }
}
