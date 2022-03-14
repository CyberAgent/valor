package jp.co.cyberagent.valor.trino;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.sdk.plan.SimplePlan;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanImpl;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ValorRecordSetProvider implements ConnectorRecordSetProvider {

  static Logger LOG = LoggerFactory.getLogger(ValorRecordSetProvider.class);

  private final String connectorId;

  private ValorConnection connection;
  private MetadataJsonSerde serde;

  @Inject
  public ValorRecordSetProvider(ValorConnectorId connectorId) {
    this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
  }

  @Override
  public RecordSet getRecordSet(ConnectorTransactionHandle transaction,
                                ConnectorSession session,
                                ConnectorSplit split,
                                ConnectorTableHandle table,
                                List<? extends ColumnHandle> columns) {
    requireNonNull(split, "partitionChunk is null");
    ValorSplit valorSplit = (ValorSplit) split;
    checkArgument(valorSplit.getConnectorId().equals(connectorId),
        "split is not for this connector");

    ImmutableList.Builder<ValorColumnHandle> handles = ImmutableList.builder();
    for (ColumnHandle handle : columns) {
      handles.add((ValorColumnHandle) handle);
    }

    LOG.info("building record set from {}", valorSplit);
    try {
      SchemaTableName schemaTableName = valorSplit.getSchemaTableName();
      Relation relation = ValorTrinoUtil.findRelation(connection, schemaTableName);
      SchemaDescriptor schemaDescriptor
          = serde.readSchema("dummy", ByteUtils.toBytes(valorSplit.getSchemaExp()));
      Schema schema = connection.getContext()
          .createStorage(schemaDescriptor).buildSchema(relation, schemaDescriptor);
      PredicativeExpression expression
          = ValorTrinoUtil.toPredicativeExpression(relation, valorSplit.getConstraint());
      Map<String, FieldComparator> comparators =
          valorSplit.getComparators().entrySet().stream().collect(Collectors.toMap(e -> e.getKey(),
              e -> FieldComparator
                  .build(e.getValue().getOperator(), e.getValue().prefix(), e.getValue().start(),
                      e.getValue().stop(), e.getValue().regexp())));
      StorageScan fragment = new StorageScanImpl(schema.getFields(), comparators);
      SchemaScan scan = new SchemaScan(relation, schema, expression);
      scan.add(fragment);
      List<ProjectionItem> items = relation.getAttributes().stream()
          .map(a -> new AttributeNameExpression(a.name(), a.type()))
          .map(a -> new ProjectionItem(a, a.getName()))
          .collect(Collectors.toList());
      SimplePlan plan = new SimplePlan(items, scan);
      ClassLoader classLoader = schema.getStorage().getClass().getClassLoader();
      TupleScanner scanner = plan.scanner(connection);
      return new ValorRecordSet(scanner, columns, classLoader);
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
  }

  public void setConnection(ValorConnection connection) {
    this.connection = connection;
    this.serde = new MetadataJsonSerde(connection.getContext());
  }
}
