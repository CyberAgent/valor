package jp.co.cyberagent.valor.sdk.formatter;

import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.BeforeEach;

public class TestMapKeyFormatter {

  private MapAttributeType mapAttributeType;

  @BeforeEach
  public void init() {
    mapAttributeType = new MapAttributeType();
    mapAttributeType.addGenericElementType(StringAttributeType.INSTANCE);
    mapAttributeType.addGenericElementType(StringAttributeType.INSTANCE);
  }

}


