package jp.co.cyberagent.valor.spi.optimize;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;

public interface Enumerator {
  default List<SchemaDescriptor> enumerateSchemas(Iterable<RelationScan> queries, ValorConf conf) {
    List<SchemaDescriptor> schemas = new ArrayList<>();
    for (RelationScan q : queries) {
      schemas.addAll(enumerateSchemasForScan(q, conf));
    }
    return schemas;
  }

  List<SchemaDescriptor> enumerateSchemasForScan(RelationScan query, ValorConf conf);

  static List<String> getAttrNamesByLayout(String layoutName, SchemaDescriptor schema) {
    return schema.getFields().stream()
        .filter(f -> f.getFieldName().equals(layoutName))
        .flatMap(a -> a.getFormatters().stream()
            .map(s -> s.getFormatter()
                .getProperties()
                .get("attr").toString()))
        .collect(Collectors.toList());
  }
}
