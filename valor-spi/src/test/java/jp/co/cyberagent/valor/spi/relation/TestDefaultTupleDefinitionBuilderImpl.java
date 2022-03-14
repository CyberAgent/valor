package jp.co.cyberagent.valor.spi.relation;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.io.IOException;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.Test;

public class TestDefaultTupleDefinitionBuilderImpl {

  @Test
  public void testAttributeOrder() throws IOException {
    Relation def;
    ImmutableRelation.Builder builder = ImmutableRelation.builder();
    def =
        builder.relationId("testOrder").addAttribute("a1", true, StringAttributeType.INSTANCE)
            .addAttribute("a2", true, StringAttributeType.INSTANCE)
            .addAttribute("a3", true, StringAttributeType.INSTANCE).build();
    assertArrayEquals(new String[] {"a1", "a2", "a3"},
        def.getAttributeNames().toArray(new String[0]));

    builder = ImmutableRelation.builder();
    def =
        builder.relationId("testOrder").addAttribute("a2", true, StringAttributeType.INSTANCE)
            .addAttribute("a3", true, StringAttributeType.INSTANCE)
            .addAttribute("a1", true, StringAttributeType.INSTANCE).build();
    assertArrayEquals(new String[] {"a2", "a3", "a1"},
        def.getAttributeNames().toArray(new String[0]));
  }
}
