package jp.co.cyberagent.valor.trino;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSink;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

public class ValorPageSink implements ConnectorPageSink {

  private final TypeManager typeManager;
  private final ValorConnection connection;
  private final ValorInsertTableHandle tableHandle;

  public ValorPageSink(ValorConnection connection, ValorInsertTableHandle tableHandle,
                       TypeManager typeManager) {
    this.connection = connection;
    this.tableHandle = tableHandle;
    this.typeManager = typeManager;
  }

  @Override
  public CompletableFuture<?> appendPage(Page page) {
    try {
      Relation relation
          = ValorTrinoUtil.findRelation(connection, tableHandle.getSchemaTableName());
      List<String> attributes = relation.getAttributeNames();
      int count = page.getPositionCount();
      List<Tuple> tuples = new ArrayList<>(count);
      for (int p = 0; p < count; p++) {
        Tuple tuple = new TupleImpl(relation);
        for (int c = 0; c < page.getChannelCount(); c++) {
          String attr = attributes.get(c);
          AttributeType type = relation.getAttributeType(attr);
          Object o = ValorTrinoUtil.blockToObject(type, page.getBlock(c), p, typeManager);
          tuple.setAttribute(attr, o);
        }
        tuples.add(tuple);
      }
      SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
      connection.insert(schemaTableName.getSchemaName(), relation.getRelationId(), tuples);
    } catch (IOException | ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
    return NOT_BLOCKED;
  }

  @Override
  public CompletableFuture<Collection<Slice>> finish() {
    return CompletableFuture.completedFuture(Collections.emptyList());
  }

  @Override
  public void abort() {
  }
}
