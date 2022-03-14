package jp.co.cyberagent.valor.hive;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.junit.jupiter.api.Test;

public class TestValorSerDe {

  @Test
  public void testMap() throws Exception {

    MapAttributeType mapAttributeType
        = MapAttributeType.create(StringAttributeType.INSTANCE, StringAttributeType.INSTANCE);

    Relation rel = ImmutableRelation.builder().relationId("r")
        .addAttribute("m", false, mapAttributeType).build();
    Tuple t = new TupleImpl(rel);
    Map<String, String> v = new HashMap<>();
    v.put("k1", null);
    v.put("k2", "v2");

    ValorSerDe serde = new ValorSerDe();
    t = serde.convert2Tuple(t, "m", v, ObjectInspectorFactory
        .getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    Map<String, String> a = (Map<String, String>) t.getAttribute("m");
    assertThat(a.keySet().contains("k1"), is(true));
    assertThat(a.get("k1"), is(nullValue()));
    assertThat(a.get("k2"), equalTo("v2"));

    serde = new ValorSerDe();
    serde.addIgnoreNullMapValue("m");
    t = serde.convert2Tuple(t, "m", v, ObjectInspectorFactory
        .getStandardMapObjectInspector(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector));
    a = (Map<String, String>) t.getAttribute("m");
    assertThat(a.keySet().contains("k1"), is(false));
    assertThat(a.get("k2"), equalTo("v2"));
  }
}
