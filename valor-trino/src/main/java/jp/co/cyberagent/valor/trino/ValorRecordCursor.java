package jp.co.cyberagent.valor.trino;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.validation.constraints.NotNull;
import jp.co.cyberagent.valor.spi.ThreadContextClassLoader;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ValorRecordCursor implements RecordCursor {

  static Logger LOG = LoggerFactory.getLogger(ValorRecordCursor.class);

  private final List<ValorColumnHandle> columns;
  private final TupleScanner runner;
  private long totalBytes = 0L;
  private long completeBytes = 0L;
  private Tuple currentTuple;
  private ClassLoader classLoader;

  private static final ObjectMapper om = new ObjectMapper();

  public ValorRecordCursor(
      List<ValorColumnHandle> columns, TupleScanner runner, ClassLoader classLoader) {
    this.columns = columns;
    this.runner = runner;
    this.classLoader = classLoader;
  }

  public long getCompletedBytes() {
    return completeBytes;
  }

  @Override
  public long getReadTimeNanos() {
    return 0;
  }

  @Override
  public Type getType(int i) {
    return columns.get(i).getColumnType();
  }

  @Override
  public boolean advanceNextPosition() {
    // set thread context class loader so that cursor can find valor-plugin classes
    try (ThreadContextClassLoader tcl = new ThreadContextClassLoader(classLoader)) {
      Tuple nextTuple = runner.next();
      if (nextTuple != null) {
        this.currentTuple = nextTuple;
      }
      return nextTuple != null;
    } catch (ValorException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
  }

  @Override
  public boolean getBoolean(int i) {
    return extractAttributeValue(i, Boolean.class);
  }

  @Override
  public long getLong(int i) {
    return extractAttributeValue(i, Long.class);
  }

  @Override
  public double getDouble(int i) {
    return extractAttributeValue(i, Double.class);
  }

  @Override
  public Slice getSlice(int i) {
    if (currentTuple == null) {
      return null;
    }
    ValorColumnHandle column = columns.get(i);

    if (ValorColumnHandle.isUpdateRowIdColumn(column)) {
      Relation relation = runner.getRelation();
      List<String> keys = relation.getKeyAttributeNames();
      if (keys.isEmpty()) {
        throw new ValorRuntimeException("relation without key attributes is not updatable");
      }
      byte[][] keyBytes = new byte[keys.size()][];
      for (int j = 0; j < keys.size(); j++) {
        String key = keys.get(j);
        AttributeType type =  relation.getAttributeType(key);
        Object v = currentTuple.getAttribute(key);
        keyBytes[j] = type.serialize(v);
      }
      byte[] updateId = ValorColumnHandle.toUpdateId(keyBytes);
      return Slices.wrappedBuffer(updateId);
    }

    Object v = currentTuple.getAttribute(column.getColumnName());
    if (v == null) {
      return null;
    }
    AttributeType type = currentTuple.getAttributeType(column.getColumnName());
    try {
      return Slices.wrappedBuffer(type.serialize(v));
    } catch (SerdeException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
  }

  @Override
  public Object getObject(int i) {
    String columnName = columns.get(i).getColumnName();
    Object obj = currentTuple.getAttribute(columnName);
    Type type = columns.get(i).getColumnType();
    return toPrestoObject(obj, type, columnName);
  }

  private static Object toPrestoObject(Object valorObject, Type prestoType, String columnName) {
    if (prestoType.getTypeSignature().getBase().equals("json")) {
      String jsonString;
      try {
        jsonString = om.writeValueAsString(valorObject);
      } catch (JsonProcessingException e) {
        throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
      }
      // JsonType can also be instanceof Map and List, so return here.
      return jsonString; // JSON type is expressed as String Slice in Presto.
    }
    if (valorObject instanceof List) {
      Type elementType = prestoType.getTypeParameters().get(0);
      BlockBuilder arrayBlockBuilder = prestoType.createBlockBuilder(null, 1);
      BlockBuilder builder = arrayBlockBuilder.beginBlockEntry();
      for (Object e : (List) valorObject) {
        TypeUtils.writeNativeValue(
            elementType, builder, toPrestoObject(e, elementType, columnName));
      }
      arrayBlockBuilder.closeEntry();
      return prestoType.getObject(arrayBlockBuilder, 0);
    }
    if (valorObject instanceof Map) {
      Type keyType = prestoType.getTypeParameters().get(0);
      Type valueType = prestoType.getTypeParameters().get(1);

      BlockBuilder mapBlockBuilder = prestoType.createBlockBuilder(null, 1);
      BlockBuilder builder = mapBlockBuilder.beginBlockEntry();
      for (Entry<?, ?> entry : ((Map<?, ?>) valorObject).entrySet()) {
        TypeUtils.writeNativeValue(
            keyType, builder, toPrestoObject(entry.getKey(), keyType, columnName));
        TypeUtils.writeNativeValue(
            valueType, builder, toPrestoObject(entry.getValue(), valueType, columnName));
      }
      mapBlockBuilder.closeEntry();
      return prestoType.getObject(mapBlockBuilder, 0);
    }
    return extractAttributeValueWithValue(valorObject, Object.class, columnName);
  }

  @Override
  public boolean isNull(int i) {
    ValorColumnHandle column = columns.get(i);
    if (ValorColumnHandle.isUpdateRowIdColumn(column)) {
      return false;
    }
    Object v = extractAttributeValue(i, Object.class);
    return v == null;
  }

  private <T> T extractAttributeValue(int i, Class<T> cls) {
    if (currentTuple == null) {
      return null;
    }
    ValorColumnHandle column = columns.get(i);
    String columnName = column.getColumnName();
    Object v = currentTuple.getAttribute(columnName);
    if (v == null) {
      return null;
    }
    return extractAttributeValueWithValue(v, cls, columnName);
  }

  private static <T> T extractAttributeValueWithValue(
      @NotNull Object value, Class<T> cls, String columnName) {
    // Although trino provides Integer, trino uses Long for Integer internally
    // Convert Integer to Long if the column type is Integer
    if (cls.isAssignableFrom(Long.class) && value.getClass().isAssignableFrom(Integer.class)) {
      return cls.cast(((Integer) value).longValue());
    }
    if (cls.isAssignableFrom(Double.class) && value.getClass().isAssignableFrom(Float.class)) {
      return cls.cast(((Float) value).doubleValue());
    }
    if (!cls.isAssignableFrom(value.getClass())) {
      throw new TrinoException(
          ValorErrorCode.VALOR_ERROR,
          String.format("value of %s (%s) is not %s", columnName, value, cls.getCanonicalName()));
    }
    return cls.cast(value);
  }

  @Override
  public void close() {
    try {
      runner.close();
    } catch (IOException e) {
      throw new TrinoException(ValorErrorCode.VALOR_ERROR, e);
    }
  }
}
