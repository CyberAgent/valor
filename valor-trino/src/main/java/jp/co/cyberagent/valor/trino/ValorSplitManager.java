package jp.co.cyberagent.valor.trino;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.connector.SchemaTableName;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.sdk.plan.NaivePrimitivePlanner;
import jp.co.cyberagent.valor.sdk.plan.PrimitivePlanner;
import jp.co.cyberagent.valor.sdk.plan.SimplePlan;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionItem;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.scanner.SchemaScan;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ValorSplitManager implements ConnectorSplitManager {

  static Logger LOG = LoggerFactory.getLogger(ValorSplitManager.class);

  private final ValorConnectorId connectorId;

  private ValorConnection conn;
  private MetadataJsonSerde serde;

  @Inject
  public ValorSplitManager(ValorConnectorId connectorId) {
    this.connectorId = connectorId;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
                                        ConnectorSession session,
                                        ConnectorTableLayoutHandle layout,
                                        SplitSchedulingStrategy splitSchedulingStrategy) {
    final ValorTableLayoutHandle layoutHandle = (ValorTableLayoutHandle) layout;
    SchemaScan scan = null;
    SchemaTableName schemaTableName = layoutHandle.getSchemaTableName();
    try {
      Relation relation = ValorTrinoUtil.findRelation(conn, schemaTableName);
      Collection<Schema> schemas
          = conn.listSchemas(schemaTableName.getSchemaName(), relation.getRelationId());
      List<ProjectionItem> items = relation.getAttributes().stream()
          .map(a -> new AttributeNameExpression(a.name(), a.type()))
          .map(a -> new ProjectionItem(a, a.getName()))
          .collect(Collectors.toList());
      PredicativeExpression condition
          = ValorTrinoUtil.toPredicativeExpression(relation, layoutHandle.getConstraint());

      PrimitivePlanner planner = new NaivePrimitivePlanner();
      ScanPlan plan = relation.getSchemaHandler().plan(conn, schemas, items, condition, planner);
      if (plan instanceof SimplePlan) {
        scan = ((SimplePlan) plan).getScan();
      } else {
        throw new ValorException("scan using multiple schema is not supported: " + plan);
      }
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }

    final Schema schema = scan.getSchema();
    final List<StorageScan> storageScans = scan.getFragments();
    List<ValorSplit> splits = new ArrayList<>(storageScans.size());
    for (StorageScan storageScan : storageScans) {
      List<StorageScan> splittedScans = splitScan(storageScan, schema);
      for (StorageScan s : splittedScans) {
        ValorSplit split = buildSplit(schema, s, layoutHandle);
        splits.add(split);
      }
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("extracted splits : {}",
          splits.stream().map(ValorSplit::toString).collect(Collectors.joining(", ")));
    }
    return new FixedSplitSource(splits);
  }

  private List<StorageScan> splitScan(StorageScan scan, Schema schema) {
    try (StorageConnection storageConn = conn.createConnection(schema)) {
      return storageConn.split(scan);
    } catch (ValorException | IOException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
  }

  private ValorSplit buildSplit(
      Schema schema, StorageScan fragment, ValorTableLayoutHandle layoutHandle) {
    Map<String, ValorSplit.FieldComparatorDesc> comparators = new HashMap<>();
    for (String field : schema.getFields()) {
      FieldComparator c = fragment.getFieldComparator(field);
      if (c == null) {
        continue;
      }
      comparators.put(field, new ValorSplit.FieldComparatorDesc(c.getOperator(), c.getPrefix(),
          c.getStart(), c.getStop(), c.getRegexp()));
    }

    String schemaExp = ByteUtils.toString(serde.serialize(SchemaDescriptor.from(schema)));
    return new ValorSplit(
        connectorId.toString(), layoutHandle.getSchemaTableName(), schemaExp,
        layoutHandle.getConstraint(), comparators);
  }

  public void setConnection(ValorConnection conn) {
    this.conn = conn;
    this.serde = new MetadataJsonSerde(conn.getContext());
  }
}
