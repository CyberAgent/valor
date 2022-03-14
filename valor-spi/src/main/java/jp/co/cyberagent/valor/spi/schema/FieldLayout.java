package jp.co.cyberagent.valor.spi.schema;

import java.util.Arrays;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(
    allParameters = true,
    depluralize = true,
    visibility = Value.Style.ImplementationVisibility.PUBLIC)
public interface FieldLayout {

  static FieldLayout of(String fieldName, Segment... formatters) {
    return of(fieldName, Arrays.asList(formatters));
  }

  static FieldLayout of(String fieldName, List<Segment> formatters) {
    return ImmutableFieldLayout.of(fieldName, formatters);
  }

  String fieldName();

  @Deprecated
  @Value.Auxiliary
  default String getFieldName() {
    return fieldName();
  }

  List<Segment> formatters();

  @Deprecated
  @Value.Auxiliary
  default List<Segment> getFormatters() {
    return formatters();
  }
}
