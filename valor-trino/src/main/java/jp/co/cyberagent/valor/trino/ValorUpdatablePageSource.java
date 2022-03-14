package jp.co.cyberagent.valor.trino;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.RecordPageSource;
import io.trino.spi.connector.UpdatablePageSource;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.Relation;

public class ValorUpdatablePageSource implements UpdatablePageSource {

  private final RecordPageSource inner;
  private final ValorTableHandle table;
  private final ValorConnection connection;
  private final String namespace;
  private final String relationId;

  private transient Relation.Attribute[] keyAttrs;

  public ValorUpdatablePageSource(
      ValorConnection connection, ValorTableHandle table, ValorRecordSet recordSet) {
    this.connection = connection;
    this.table = table;
    this.inner = new RecordPageSource(recordSet);
    this.namespace = table.getSchemaTableName().getSchemaName();
    this.relationId = table.getSchemaTableName().getTableName();
    try {
      Relation relation = connection.findRelation(namespace, relationId);
      List<String> keys = relation.getKeyAttributeNames();
      this.keyAttrs = new Relation.Attribute[keys.size()];
      for (int i = 0; i < keys.size(); i++) {
        this.keyAttrs[i] = relation.getAttribute(keys.get(i));
      }
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
  }

  @Override
  public void deleteRows(Block rowIds) {
    try {
      for (int i = 0; i < rowIds.getPositionCount(); i++) {
        int len = rowIds.getSliceLength(i);
        Slice slice = rowIds.getSlice(i, 0, len);
        Map<Relation.Attribute, Object> c =
            ValorColumnHandle.fromUpdateId(keyAttrs, slice.getBytes());
        List<PredicativeExpression> cond = c.entrySet().stream().map(e -> new EqualOperator(
            new AttributeNameExpression(e.getKey().name(), e.getKey().type()),
            new ConstantExpression(e.getValue()))).collect(
            Collectors.toList());
        // TODO batch delete
        connection.delete(namespace, relationId, AndOperator.join(cond));
      }

    } catch (ValorException | IOException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
  }

  @Override
  public CompletableFuture<Collection<Slice>> finish() {
    CompletableFuture<Collection<Slice>> cf = new CompletableFuture<>();
    cf.complete(Collections.emptyList());
    return cf;
  }

  @Override
  public long getCompletedBytes() {
    return inner.getCompletedBytes();
  }

  @Override
  public long getReadTimeNanos() {
    return inner.getReadTimeNanos();
  }

  @Override
  public boolean isFinished() {
    return inner.isFinished();
  }

  @Override
  public Page getNextPage() {
    return inner.getNextPage();
  }

  @Override
  public long getSystemMemoryUsage() {
    return inner.getSystemMemoryUsage();
  }

  @Override
  public void close() throws IOException {
    inner.close();
  }
}
