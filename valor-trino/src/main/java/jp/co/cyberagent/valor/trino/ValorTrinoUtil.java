package jp.co.cyberagent.valor.trino;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.slice.Slice;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.SqlVarbinary;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.IsNullOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanorequalOperator;
import jp.co.cyberagent.valor.sdk.plan.function.NotEqualOperator;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.OrOperator;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.ArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.ByteArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.Bytes;
import jp.co.cyberagent.valor.spi.relation.type.DoubleAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.FloatAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.JsonAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;


/**
 *
 */
public class ValorTrinoUtil {

  private static final ObjectMapper om = new ObjectMapper();

  public static Collection<String> listRelationIds(ValorConnection conn, String schema)
      throws ValorException {
    return conn.listRelationsIds(ValorMetadata.DEFAULT_SCHEMA.equals(schema) ? null : schema);
  }

  public static Relation findRelation(ValorConnection conn, SchemaTableName schemaTableName)
      throws ValorException {
    return findRelation(conn, schemaTableName.getSchemaName(), schemaTableName.getTableName());
  }

  public static Relation findRelation(ValorConnection conn, String schema, String table)
      throws ValorException {
    String namespace = ValorMetadata.DEFAULT_SCHEMA.equals(schema) ? null : schema;
    Collection<String> relIds = conn.listRelationsIds(namespace);
    Optional<String> caseSensitiveId
        = relIds.stream().filter(r -> r.equalsIgnoreCase(table)).findFirst();
    if (caseSensitiveId.isPresent()) {
      return conn.findRelation(namespace, caseSensitiveId.get());
    } else {
      return null;
    }
  }

  public static Type toTrinoType(AttributeType valorType, TypeManager typeManager) {
    if (valorType instanceof StringAttributeType) {
      return VarcharType.VARCHAR;
    }
    if (valorType instanceof IntegerAttributeType) {
      return IntegerType.INTEGER;
    }
    if (valorType instanceof LongAttributeType) {
      return BigintType.BIGINT;
    }
    if (valorType instanceof FloatAttributeType) {
      // TODO change to Float if support by trino (https://github.com/trinodb/trino/issues/7184)
      return DoubleType.DOUBLE;
    }
    if (valorType instanceof DoubleAttributeType) {
      return DoubleType.DOUBLE;
    }
    if (valorType instanceof ByteArrayAttributeType) {
      return VarbinaryType.VARBINARY;
    }
    if (valorType instanceof ArrayAttributeType) {
      return createArrayType((ArrayAttributeType) valorType, typeManager);
    }
    if (valorType instanceof MapAttributeType) {
      return createMapType((MapAttributeType) valorType, typeManager);
    }
    if (valorType instanceof JsonAttributeType) {
      return typeManager.getType(new TypeSignature(StandardTypes.JSON));
    }
    throw new UnsupportedOperationException(valorType.getName()
        + " is not supported in valor-presto plugin");
  }

  public static AttributeType toValorType(Type prestoType) {
    if (prestoType instanceof VarcharType) {
      return StringAttributeType.INSTANCE;
    }
    if (prestoType instanceof BigintType) {
      return LongAttributeType.INSTANCE;
    }
    if (prestoType instanceof IntegerType) {
      return IntegerAttributeType.INSTANCE;
    }
    if (prestoType instanceof DoubleType) {
      return DoubleAttributeType.INSTANCE;
    }
    if (prestoType instanceof VarbinaryType) {
      return ByteArrayAttributeType.INSTANCE;
    }
    if (prestoType instanceof ArrayType) {
      ArrayType arrayPrestoType = (ArrayType) prestoType;
      AttributeType elementType = toValorType(arrayPrestoType.getElementType());
      return ArrayAttributeType.create(elementType);
    }
    if (prestoType instanceof MapType) {
      MapType mapPrestoType = (MapType) prestoType;
      AttributeType keyType = toValorType(mapPrestoType.getKeyType());
      AttributeType valueType = toValorType(mapPrestoType.getValueType());
      return MapAttributeType.create(keyType, valueType);
    }
    if (prestoType.getDisplayName().equals(StandardTypes.JSON)) {
      return JsonAttributeType.INSTANCE;
    }
    throw new UnsupportedOperationException(
        prestoType.getDisplayName() + " is not support in valor");
  }

  public static Object blockToObject(
      AttributeType valorType, Block block, int position, TypeManager typeManager) {
    Type trinoType = toTrinoType(valorType, typeManager);
    Object o = trinoType.getObjectValue(null, block, position);
    return convertTrinoObject(valorType, o);
  }

  private static ArrayType createArrayType(ArrayAttributeType valorType, TypeManager typeManager) {
    Type elementType = toTrinoType(valorType.getElementType(), typeManager);
    return new ArrayType(elementType);
  }

  private static MapType createMapType(MapAttributeType valorType, TypeManager typeManager) {
    Type keyType = toTrinoType(valorType.getKeyType(), typeManager);
    Type valueType = toTrinoType(valorType.getValueType(), typeManager);
    return new MapType(keyType, valueType, typeManager.getTypeOperators());
  }

  public static PredicativeExpression toPredicativeExpression(
      Relation relation, TupleDomain<ColumnHandle> condition) {
    Optional<List<TupleDomain.ColumnDomain<ColumnHandle>>> optDomain = condition.getColumnDomains();
    if (!optDomain.isPresent()) {
      return null;
    }

    List<TupleDomain.ColumnDomain<ColumnHandle>> domains = optDomain.get();
    List<PredicativeExpression> expressions = new ArrayList<>();
    for (TupleDomain.ColumnDomain columnDomain : domains) {
      final ValorColumnHandle column = (ValorColumnHandle) columnDomain.getColumn();
      final String columnName = column.getColumnName();
      final AttributeType attrType = relation.getAttributeType(columnName);
      final AttributeNameExpression attrName = new AttributeNameExpression(columnName, attrType);
      final Domain domain = columnDomain.getDomain();
      List<PredicativeExpression> domExps = new ArrayList<>();
      domain.getValues().getValuesProcessor().consume(
          range -> {
            List<PredicativeExpression> or = range.getOrderedRanges().stream()
                .map(new Range2Expression(attrName)).collect(Collectors.toList());
            if (!or.isEmpty()) {
              domExps.add(OrOperator.join(or));
            }
          },
          values -> {
            List<PredicativeExpression> or = values.isInclusive()
                ? values.getValues().stream().map(v -> new EqualOperator(attrName,
                new ConstantExpression(convertTrinoObject(attrType, v))))
                .collect(Collectors.toList()) :
                values.getValues().stream().map(v -> new NotEqualOperator(attrName,
                    new ConstantExpression(convertTrinoObject(attrType, v))))
                    .collect(Collectors.toList());
            if (!or.isEmpty()) {
              domExps.add(OrOperator.join(or));
            }
          },
          allOrNone -> {
            domExps.add(allOrNone.isAll() ? ConstantExpression.TRUE : ConstantExpression.FALSE);
          }
      );
      if (domain.isNullAllowed()) {
        if (domExps.isEmpty()) {
          expressions.add(new IsNullOperator(attrName));
        } else {
          expressions.add(OrOperator.join(new IsNullOperator(attrName), AndOperator.join(domExps)));
        }
      } else if (!domExps.isEmpty()) {
        expressions.add(AndOperator.join(domExps));
      }
    }
    if (expressions.isEmpty()) {
      return null;
    }
    return AndOperator.join(expressions);
  }

  static Object convertTrinoObject(AttributeType type, Object trinoObject) {
    if (trinoObject instanceof Slice) {
      if (type instanceof StringAttributeType) {
        return ((Slice) trinoObject).toStringUtf8();
      }
      if (type instanceof ByteArrayAttributeType) {
        return new Bytes(((Slice) trinoObject).getBytes());
      }
      if (type instanceof JsonAttributeType) {
        try {
          return om.readTree(((Slice) trinoObject).toStringUtf8());
        } catch (JsonProcessingException e) {
          throw new IllegalArgumentException(String.format(
                  "prestoObject %s cannot be converted to JSON Object", trinoObject), e);
        }
      }
      throw new IllegalArgumentException("string or json is expected for slice object");
    }
    if (trinoObject instanceof SqlVarbinary) {
      return new Bytes(((SqlVarbinary) trinoObject).getBytes());
    }
    if (trinoObject instanceof Number) {
      return convertNumberObject(type, (Number)trinoObject);
    }
    return trinoObject;
  }

  private static Object convertNumberObject(AttributeType type, Number obj) {
    if (type instanceof IntegerAttributeType) {
      return obj.intValue();
    }
    if (type instanceof LongAttributeType) {
      return obj.longValue();
    }
    if (type instanceof DoubleAttributeType) {
      return obj.doubleValue();
    }
    if (type instanceof FloatAttributeType) {
      return obj.floatValue();
    }
    throw new IllegalArgumentException(
        String.format("%s is not expected for %s object (%s)", type, obj.getClass(), obj));
  }

  static class Range2Expression implements Function<Range, PredicativeExpression> {

    final AttributeNameExpression attr;
    final AttributeType type;

    public Range2Expression(AttributeNameExpression attr) {
      this.attr = attr;
      this.type = attr.getType();
    }

    @Override
    public PredicativeExpression apply(Range range) {
      ConstantExpression high = range.getHighValue()
          .map(o -> new ConstantExpression(convertTrinoObject(type, o))).orElse(null);
      ConstantExpression low = range.getLowValue()
          .map(o -> new ConstantExpression(convertTrinoObject(type, o))).orElse(null);
      if (high != null && low != null) {
        if (high.equals(low)) {
          return new EqualOperator(attr, high);
        } else {
          return AndOperator.join(
              new LessthanorequalOperator(attr, high), new GreaterthanorequalOperator(attr, low));
        }
      } else if (high == null && low == null) {
        return ConstantExpression.TRUE;
      }
      return high == null ? new GreaterthanorequalOperator(attr, low) :
          new LessthanorequalOperator(attr, high);
    }
  }
}
