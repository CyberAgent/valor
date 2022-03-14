package jp.co.cyberagent.valor.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorPropertiesConf;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PredicativeExpression;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
    serdeConstants.FIELD_DELIM, serdeConstants.COLLECTION_DELIM, serdeConstants.MAPKEY_DELIM,
    serdeConstants.SERIALIZATION_FORMAT, serdeConstants.SERIALIZATION_NULL_FORMAT,
    serdeConstants.SERIALIZATION_ESCAPE_CRLF, serdeConstants.SERIALIZATION_LAST_COLUMN_TAKES_REST,
    serdeConstants.ESCAPE_CHAR, serdeConstants.SERIALIZATION_ENCODING,})
public class ValorSerDe extends AbstractSerDe {

  public static final String HIVE_RELATION = "valor.hive.relation";
  public static final String HIVE_IGNORE_NULL_MAPVALUE = "valor.hive.map.ignorenullvalue";

  static Logger LOG = LoggerFactory.getLogger(ValorSerDe.class);
  protected ObjectInspector rowOI;
  protected TupleStruct row;
  protected List<String> columnNames;
  protected Set<String> registeredColumns;
  protected long partialMatchedRows = 0;
  protected long nextPartialMatchedRows = 1;
  private Relation relation;
  private List<TypeInfo> columnTypes;
  private List<String> ignoreNullMapValue = new ArrayList<>();

  public static PredicativeExpression extractCondition(Relation relation, Configuration jobConf) {
    String filterExprSerialized = jobConf.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (filterExprSerialized == null) {
      return null;
    }
    ExprNodeDesc filterExpr = SerializationUtilities.deserializeExpression(filterExprSerialized);
    return new ExprNodeToExpressionVisitor(relation, jobConf).walk(filterExpr);
  }

  @Override
  public void initialize(Configuration conf, Properties tableProps) throws SerDeException {
    String ignoreNullMap = conf.get(HIVE_IGNORE_NULL_MAPVALUE);
    if (ignoreNullMap != null) {
      this.ignoreNullMapValue.addAll(Arrays.asList(ignoreNullMap.split(",")));
      LOG.info("ignore null value in {}", ignoreNullMapValue);
    }
    ValorContext context = StandardContextFactory.create(new ValorPropertiesConf(tableProps));
    try (ValorConnection client = ValorConnectionFactory.create(context)) {
      this.relation = client.findRelation(tableProps.getProperty(HIVE_RELATION));
    } catch (IOException | ValorException e) {
      throw new SerDeException(e);
    }
    initializeColumns(conf, tableProps);
  }

  protected void initializeColumns(Configuration conf, Properties tbl) throws SerDeException {
    // We can get the table definition from tbl.
    // Read the configuration parameters
    String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);
    this.columnNames = Arrays.asList(columnNameProperty.split(","));
    this.columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
    assert columnNames.size() == columnTypes.size();
    int numColumns = columnNames.size();

    this.registeredColumns = new HashSet<>();
    List<ObjectInspector> columnOIs = new ArrayList<ObjectInspector>(columnNames.size());
    for (int c = 0; c < numColumns; c++) {
      TypeInfo colType = columnTypes.get(c);
      this.registeredColumns.add(this.columnNames.get(c));
      columnOIs.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(colType));
    }
    rowOI = ObjectInspectorFactory.getStandardStructObjectInspector(columnNames, columnOIs);

    // Constructing the row object, etc, which will be reused for all rows.
    row = new TupleStruct(numColumns);
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return rowOI;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return TupleWritable.class;
  }

  protected long getNextNumberToDisplay(long now) {
    return now * 10;
  }

  @Override
  public Object deserialize(Writable blob) throws SerDeException {
    if (!(blob instanceof TupleWritable)) {
      throw new SerDeException("the value should be a TupleWritable");
    }

    Tuple value = null;
    try {
      value = ((TupleWritable) blob).convertFromWritable(relation);
    } catch (IOException e) {
      throw new SerDeException(e);
    }
    Map<String, Object> result = new HashMap<String, Object>();
    for (String attrName : value.getAttributeNames()) {
      if (this.columnNames.contains(attrName)) {
        result.put(attrName, value.getAttribute(attrName));
      }
      // else {not target attribute}
    }

    // Otherwise, return the row.
    for (int c = 0; c < this.columnNames.size(); c++) {
      try {
        String colName = this.columnNames.get(c);
        if (result.containsKey(colName)) {
          row.set(c, result.get(colName));
        } else {
          row.set(c, null);
        }
      } catch (RuntimeException e) {
        partialMatchedRows++;
        if (partialMatchedRows >= nextPartialMatchedRows) {
          nextPartialMatchedRows = getNextNumberToDisplay(nextPartialMatchedRows);
          // Report the row
          LOG.warn(
              partialMatchedRows + " partially unmatched rows are found, cannot find group " + c
                  + ": " + value);
        }
        row.set(c, null);
      }
    }
    return row.getFieldsAsList();
  }

  @Override
  public Writable serialize(Object obj, ObjectInspector oi) throws SerDeException {
    if (oi.getCategory() != Category.STRUCT) {
      throw new SerDeException(
          getClass().toString() + " can only serialize struct types, but we got: "
              + oi.getTypeName());
    }

    // Prepare the field ObjectInspectors
    StructObjectInspector soi = (StructObjectInspector) oi;
    List<? extends StructField> fields = soi.getAllStructFieldRefs();
    List<Object> list = soi.getStructFieldsDataAsList(obj);

    Tuple tuple = new TupleImpl(relation);
    // Serialize each field
    for (int i = 0; i < fields.size(); i++) {
      // Append the separator if needed.
      // Get the field objectInspector and the field object.
      ObjectInspector foi = fields.get(i).getFieldObjectInspector();
      String attrName = this.columnNames.get(i);
      Object f = (list == null ? null : list.get(i));
      if (f == null) {
        tuple.setAttribute(attrName, null);
      } else {
        tuple = convert2Tuple(tuple, attrName, f, foi);
      }
    }
    try {
      return new TupleWritable(this.relation, tuple);
    } catch (IOException e) {
      throw new SerDeException(e);
    }
  }

  @VisibleForTesting
  public Tuple convert2Tuple(Tuple tuple, String attrname, Object f, ObjectInspector foi)
      throws SerDeException {
    switch (foi.getCategory()) {
      case PRIMITIVE:
        PrimitiveObjectInspector poi = (PrimitiveObjectInspector) foi;
        tuple.setAttribute(attrname, poi.getPrimitiveJavaObject(f));
        return tuple;
      case MAP:
        MapObjectInspector moi = (MapObjectInspector) foi;
        PrimitiveObjectInspector koi = (PrimitiveObjectInspector) moi.getMapKeyObjectInspector();
        PrimitiveObjectInspector voi = (PrimitiveObjectInspector) moi.getMapValueObjectInspector();
        Map<?, ?> map = moi.getMap(f);
        Map<Object, Object> tmpMap = Maps.newHashMap();
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          Object keyObj = koi.getPrimitiveJavaObject(entry.getKey());
          Object valueObj = voi.getPrimitiveJavaObject(entry.getValue());
          if (valueObj != null || !ignoreNullMapValue.contains(attrname)) {
            tmpMap.put(keyObj, valueObj);
          }
        }
        tuple.setAttribute(attrname, tmpMap);
        return tuple;
      case LIST:
        ListObjectInspector loi = (ListObjectInspector) foi;
        PrimitiveObjectInspector eoi =
            (PrimitiveObjectInspector) loi.getListElementObjectInspector();
        List<?> list = loi.getList(f);
        List<Object> tmpList = Lists.newArrayList();
        for (Object e : list) {
          Object elmObj = eoi.getPrimitiveJavaObject(e);
          tmpList.add(elmObj);
        }
        tuple.setAttribute(attrname, tmpList);
        return tuple;
      case STRUCT:
      case UNION:
      default:
        throw new SerDeException("unsupported inspector type:" + foi.getCategory());
    }
  }

  @Override
  public SerDeStats getSerDeStats() {
    // TODO Auto-generated method stub
    return null;
  }

  @VisibleForTesting
  public void addIgnoreNullMapValue(String attr) {
    this.ignoreNullMapValue.add(attr);
  }
}
