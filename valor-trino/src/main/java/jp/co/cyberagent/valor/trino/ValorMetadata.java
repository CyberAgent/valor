package jp.co.cyberagent.valor.trino;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorOutputMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableLayout;
import io.trino.spi.connector.ConnectorTableLayoutHandle;
import io.trino.spi.connector.ConnectorTableLayoutResult;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarbinaryType;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.metadata.MetadataSerde;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ValorMetadata implements ConnectorMetadata {

  public static final String DEFAULT_SCHEMA = "default";

  public static final String DEFAULT_VALOR_SCHEMA_ID = "trinodefault";

  static Logger LOG = LoggerFactory.getLogger(ValorMetadata.class);
  private final ValorConnectorId connectorId;
  private final TypeManager typeManager;

  private ValorConnection connection;

  private final MetadataSerde serde;

  @Inject
  public ValorMetadata(ValorConnectorId connectorId, ValorContext context,
                       TypeManager typeManager) {
    this.connectorId = connectorId;
    this.serde = new MetadataJsonSerde(context);
    this.typeManager = typeManager;
  }

  @Override
  public List<String> listSchemaNames(ConnectorSession connectorSession) {
    List<String> prestoSchemas = new ArrayList<>(connection.getNamespaces());
    prestoSchemas.add(DEFAULT_SCHEMA);
    return prestoSchemas;
  }

  @Override
  public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession,
                                             SchemaTableName schemaTableName) {
    try {
      if (ValorTrinoUtil.findRelation(connection, schemaTableName) == null) {
        return null;
      }
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
    return new ValorTableHandle(connectorId.toString(), schemaTableName);
  }

  @Override
  public List<ConnectorTableLayoutResult> getTableLayouts(ConnectorSession session,
                                                          ConnectorTableHandle tableHandle,
                                                          Constraint constraint,
                                                          Optional<Set<ColumnHandle>> optional) {
    ValorTableHandle valorTableHandle = (ValorTableHandle) tableHandle;
    ConnectorTableLayout layout =
        new ConnectorTableLayout(new ValorTableLayoutHandle(connectorId.toString(),
            valorTableHandle.getSchemaTableName(),
            constraint.getSummary()));
    return ImmutableList.of(new ConnectorTableLayoutResult(layout, constraint.getSummary()));
  }

  @Override
  public ConnectorTableLayout getTableLayout(ConnectorSession session,
                                             ConnectorTableLayoutHandle tableLayoutHandle) {
    return new ConnectorTableLayout(tableLayoutHandle);
  }

  @Override
  public ConnectorTableMetadata getTableMetadata(ConnectorSession connectorSession,
                                                 ConnectorTableHandle connectorTableHandle) {
    ValorTableHandle valorTableHandle = (ValorTableHandle) connectorTableHandle;
    try {
      final Relation relation =
          ValorTrinoUtil.findRelation(connection, valorTableHandle.getSchemaTableName());
      List<ColumnMetadata> columns =
          relation.getAttributeNames().stream().map(a -> new ColumnMetadata(a,
              ValorTrinoUtil.toTrinoType(relation.getAttributeType(a), typeManager)))
              .collect(Collectors.toList());
      return new ConnectorTableMetadata(valorTableHandle.getSchemaTableName(), columns);
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
  }

  @Override
  public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
    String schema = schemaName.isPresent() ? schemaName.get() : DEFAULT_SCHEMA;
    try {
      return ValorTrinoUtil.listRelationIds(connection, schema)
          .stream().map(r -> new SchemaTableName(schema, r)).collect(Collectors.toList());
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, "failed to get relation list", e);
    }
  }

  @Override
  public Map<String, ColumnHandle> getColumnHandles(ConnectorSession connectorSession,
                                                    ConnectorTableHandle connectorTableHandle) {
    ValorTableHandle valorTableHandle = (ValorTableHandle) connectorTableHandle;
    try {
      final Relation relation =
          ValorTrinoUtil.findRelation(connection, valorTableHandle.getSchemaTableName());
      return relation.getAttributeNames().stream().collect(Collectors.toMap(Function.identity(),
          a -> new ValorColumnHandle(connectorId.toString(), a,
              ValorTrinoUtil.toTrinoType(relation.getAttributeType(a), typeManager))));
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
  }

  @Override
  public ColumnMetadata getColumnMetadata(ConnectorSession connectorSession,
                                          ConnectorTableHandle connectorTableHandle,
                                          ColumnHandle columnHandle) {
    ValorColumnHandle ch = (ValorColumnHandle) columnHandle;
    return new ColumnMetadata(ch.getColumnName(), ch.getColumnType());
  }

  @Override
  public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
      ConnectorSession connectorSession, SchemaTablePrefix schemaTablePrefix) {
    String schema = schemaTablePrefix.getSchema().orElse(DEFAULT_SCHEMA);
    Optional<String> tableName = schemaTablePrefix.getTable();

    try {
      Collection<String> tables = tableName.isPresent()
          ? Arrays.asList(tableName.get()) : ValorTrinoUtil.listRelationIds(connection, schema);
      Map<SchemaTableName, List<ColumnMetadata>> tableColumns = new HashMap<>();
      for (String table : tables) {
        tableColumns.put(
            new SchemaTableName(schema, table), columns(schemaTablePrefix.toSchemaTableName()));
      }
      return tableColumns;
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, "failed to get list of tables", e);
    }
  }

  private List<ColumnMetadata> columns(SchemaTableName table) throws ValorException {
    Relation relation = ValorTrinoUtil.findRelation(connection, table);
    List<ColumnMetadata> columns = new ArrayList<>(relation.getAttributeNames().size());
    for (String attr : relation.getAttributeNames()) {
      columns.add(new ColumnMetadata(attr,
          ValorTrinoUtil.toTrinoType(relation.getAttributeType(attr), typeManager)));
    }
    return columns;
  }

  @Override
  public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata,
                          boolean ignoreExisting) {
    Object keyProp = tableMetadata.getProperties().get(ValorConnector.TABLE_PROPERTY_KEYS);
    Collection<String> keys = keyProp == null ? Collections.emptySet() :
        Arrays.asList(((String) keyProp).split(","));
    ImmutableRelation.Builder relBuilder = ImmutableRelation.builder();
    relBuilder.relationId(tableMetadata.getTable().getTableName());
    tableMetadata.getColumns().forEach(c -> relBuilder.addAttribute(c.getName(),
        keys.contains(c.getName()), ValorTrinoUtil.toValorType(c.getType())));

    SchemaDescriptor schema = null;
    Object schemaProp = tableMetadata.getProperties().get(ValorConnector.TABLE_PROPERTY_SCHEMA);
    if (schemaProp != null) {
      try (StringReader reader = new StringReader((String) schemaProp)) {
        schema = serde.readSchema(DEFAULT_VALOR_SCHEMA_ID, reader);
      } catch (ValorRuntimeException e) {
        throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
      }
    }
    try {
      connection.createRelation(relBuilder.build(), ignoreExisting);
      if (schema != null) {
        connection.createSchema(tableMetadata.getTable().getTableName(), schema, ignoreExisting);
      }
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
    // TODO init storage table
  }

  @Override
  public ConnectorInsertTableHandle beginInsert(ConnectorSession session,
                                                ConnectorTableHandle tableHandle) {
    ValorTableHandle valorTableHandle = (ValorTableHandle) tableHandle;
    List<ColumnMetadata> columns = null;
    try {
      columns = columns(valorTableHandle.getSchemaTableName());
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
    return new ValorInsertTableHandle(valorTableHandle.getConnectorId(),
        valorTableHandle.getSchemaTableName(),
        columns.stream().map(c -> new ValorColumnHandle(valorTableHandle.getConnectorId(),
            c.getName(), c.getType())).collect(Collectors.toList())
    );
  }

  @Override
  public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
                                                        ConnectorInsertTableHandle insertHandle,
                                                        Collection<Slice> fragments,
                                                        Collection<ComputedStatistics> stats) {
    return Optional.empty();
  }

  @Override
  public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session,
                                                 ConnectorTableHandle tableHandle) {
    return new ValorColumnHandle(
        connectorId.toString(), ValorColumnHandle.UPDATE_ROW_ID, VarbinaryType.VARBINARY);

  }

  @Override
  public ConnectorTableHandle beginDelete(ConnectorSession session,
                                          ConnectorTableHandle tableHandle) {
    return tableHandle;
  }

  @Override
  public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle,
                           Collection<Slice> fragments) {
  }

  public void setConnection(ValorConnection connection) {
    this.connection = connection;
  }
}
