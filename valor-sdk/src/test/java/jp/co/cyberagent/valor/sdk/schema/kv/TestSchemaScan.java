package jp.co.cyberagent.valor.sdk.schema.kv;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanImpl;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestSchemaScan {

  static final String KEY_ATTR = "k";
  static final String COL_ATTR = "c";
  static final String VAL_ATTR = "v";
  static final List<String> FIELDS = Arrays.asList(KEY_ATTR, COL_ATTR, VAL_ATTR);
  static final Relation relation = ImmutableRelation.builder().relationId("r")
      .addAttribute(KEY_ATTR, true, StringAttributeType.INSTANCE)
      .addAttribute(COL_ATTR, true, StringAttributeType.INSTANCE)
      .addAttribute(VAL_ATTR, false, StringAttributeType.INSTANCE)
      .build();


  @Mock
  private Schema schema;

  @BeforeEach
  public void initMock() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testAdd() {

    byte[] keyPrefix = new byte[] {0x05};
    Map<String, FieldComparator> keyCondition = new HashMap<>();
    keyCondition.put(KEY_ATTR, FieldComparator.build(FieldComparator.Operator.EQUAL, keyPrefix,
        keyPrefix, ByteUtils.unsignedCopyAndIncrement(keyPrefix), null));
    StorageScan ss1 = new StorageScanImpl(FIELDS, keyCondition);

    byte[] valPrefix = new byte[] {0x03};
    Map<String, FieldComparator> valCondition = new HashMap<>();
    valCondition.put(VAL_ATTR, FieldComparator.build(FieldComparator.Operator.EQUAL, valPrefix,
        null, null, null));
    StorageScan ss2 = new StorageScanImpl(FIELDS, valCondition);

    when(schema.getFields()).thenReturn(FIELDS);

    SchemaScan scan = new SchemaScan(relation, schema, null);
    scan.add(ss1);
    scan.add(ss2);
    assertEquals(1, scan.getFragments().size());
  }

  @Test
  public void testAddSameKey() {
    byte[] keyPrefix = new byte[] {0x01};
    byte[] colPrefix = new byte[] {0x05};
    Map<String, FieldComparator> colCondition = new HashMap<>();
    colCondition.put(KEY_ATTR, FieldComparator.build(FieldComparator.Operator.EQUAL, keyPrefix,
        keyPrefix, ByteUtils.unsignedCopyAndIncrement(keyPrefix), null));
    colCondition.put(COL_ATTR, FieldComparator.build(FieldComparator.Operator.EQUAL, colPrefix,
        colPrefix, ByteUtils.unsignedCopyAndIncrement(colPrefix), null));
    StorageScan ss1 = new StorageScanImpl(FIELDS, colCondition);

    byte[] valPrefix = new byte[] {0x03};
    Map<String, FieldComparator> valCondition = new HashMap<>();
    valCondition.put(KEY_ATTR, FieldComparator.build(FieldComparator.Operator.EQUAL, keyPrefix,
        keyPrefix, ByteUtils.unsignedCopyAndIncrement(keyPrefix), null));
    valCondition.put(VAL_ATTR, FieldComparator.build(FieldComparator.Operator.EQUAL, valPrefix,
        null, null, null));
    StorageScan ss2 = new StorageScanImpl(FIELDS, valCondition);

    when(schema.getFields()).thenReturn(FIELDS);

    SchemaScan scan = new SchemaScan(relation, schema, null);
    scan.add(ss1);
    scan.add(ss2);
    assertEquals(1, scan.getFragments().size());
  }
}
