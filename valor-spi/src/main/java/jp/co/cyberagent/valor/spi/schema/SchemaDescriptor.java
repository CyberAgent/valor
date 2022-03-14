package jp.co.cyberagent.valor.spi.schema;

import java.util.List;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(deepImmutablesDetection = true, depluralize = true)
public interface SchemaDescriptor {

  String getStorageClassName();

  ValorConf getStorageConf();

  String getSchemaId();

  List<FieldLayout> getFields();

  @Value.Default
  default boolean isPrimary() {
    return false;
  }

  @Value.Default
  default Schema.Mode getMode() {
    return Schema.Mode.PUBLIC;
  }

  @Value.Default
  default ValorConf getConf() {
    return new ValorConfImpl();
  }

  static SchemaDescriptor from(Schema schema) {
    List<String> fields = schema.getFields();
    return ImmutableSchemaDescriptor.builder()
        .storageClassName(schema.getStorage().getClass().getCanonicalName())
        .storageConf(schema.getStorage().getConf())
        .schemaId(schema.getSchemaId())
        .fields(fields.stream().map(schema::getLayout).collect(Collectors.toList()))
        .isPrimary(schema.isPrimary())
        .mode(schema.getMode())
        .conf(schema.getConf())
        .build();
  }

}
