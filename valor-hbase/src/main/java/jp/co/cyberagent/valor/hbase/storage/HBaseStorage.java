package jp.co.cyberagent.valor.hbase.storage;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import jp.co.cyberagent.valor.hbase.util.HBaseUtil;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.storage.kv.KeyValueStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfParam;
import jp.co.cyberagent.valor.spi.exception.IllegalSchemaException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseStorage extends KeyValueStorage implements AutoCloseable {

  public static final String NAME = "hbase";

  public static final String NAMESPACE_PARAM_KEY = "valor.hbase.namespace";
  public static final String TABLE_PARAM_KEY = "valor.hbase.table";
  // used in HBaseDefaultSchemaHandler
  public static final String FAMILY_PARAM_KEY = "valor.hbase.family";
  public static final String QUALIFIER_PARAM_KEY = "valor.hbase.qualifier";

  public static final String HBASE_CONF_KEY = "valor.hbase.conf";

  public static final ValorConfParam TABLE_PARAM = new ValorConfParam(TABLE_PARAM_KEY, null);
  public static final ValorConfParam NAMESPACE_PARAM
      = new ValorConfParam(NAMESPACE_PARAM_KEY,null);

  public static final List<String> FIELDS_WITH_TIMESTAMP = Arrays
      .asList(HBaseCell.TABLE, HBaseCell.ROWKEY,
          HBaseCell.FAMILY, HBaseCell.QUALIFIER, HBaseCell.TIMESTAMP, HBaseCell.VALUE);

  public static final List<String> FIELDS = Arrays.asList(HBaseCell.TABLE, HBaseCell.ROWKEY,
      HBaseCell.FAMILY, HBaseCell.QUALIFIER, HBaseCell.VALUE);

  public static final List<String> KEY_FIELDS = Arrays.asList(HBaseCell.TABLE, HBaseCell.ROWKEY,
      HBaseCell.FAMILY, HBaseCell.QUALIFIER);

  protected AtomicReference<Connection> connection;

  public HBaseStorage(ValorConf conf) {
    super(conf);
    connection = new AtomicReference<>();
  }

  protected Connection getConnection() {
    Connection hconn = connection.get();
    if (hconn != null) {
      return hconn;
    }
    Configuration hconf = HBaseUtil.toConfiguration(conf);
    try {
      hconn = ConnectionFactory.createConnection(hconf);
      if (!connection.compareAndSet(null, hconn)) {
        hconn.close();
      }
    } catch (IOException e) {
      throw new ValorRuntimeException("failed to create HBase connection", e);
    }
    return connection.get();
  }

  @Override
  protected StorageConnectionFactory getConnectionFactory(Relation relation,
                                                          SchemaDescriptor descriptor) {
    throw new UnsupportedOperationException("should not be called");
  }

  @Override
  public void close() throws Exception {
    // TODO implement cleaner
    Connection hconn = connection.get();
    if (hconn != null) {
      hconn.close();
    }
  }

  protected static String parseConstant(String field, FieldLayout layout) {
    List<Segment> formatters = layout.getFormatters();
    Segment formatter = formatters.get(0);
    if (formatters.size() != 1 && !(formatter instanceof ConstantFormatter)) {
      throw new UnsupportedOperationException(
          field + " should be composed of one constant formatter");
    }
    byte[] v = ((ConstantFormatter) formatter).getValue();
    return new String(v);
  }


  @Override
  public Schema buildSchema(Relation relation, SchemaDescriptor descriptor) throws ValorException {
    List<FieldLayout> layoutDescriptors = descriptor.getFields();

    ValorConf storageConf = descriptor.getStorageConf();
    int nextFieldIndex = 0;
    FieldLayout layout = layoutDescriptors.get(nextFieldIndex++);
    String table;
    if (HBaseCell.TABLE.equals(layout.getFieldName())) {
      table = parseConstant(HBaseCell.TABLE, layout);
      storageConf.set(HBaseStorage.TABLE_PARAM_KEY, table);
      layout = layoutDescriptors.get(nextFieldIndex++);
    } else {
      table = HBaseStorage.TABLE_PARAM.get(storageConf);
      if (table == null) {
        throw new IllegalSchemaException(relation.getRelationId(), descriptor.getSchemaId(),
            "table name is not set");
      }
    }
    CellSchemaBuilder parser = new CellSchemaBuilder();
    parser.setRelation(relation);
    parser.setStorageClass(HBaseStorage.class.getCanonicalName());
    parser.setStorageConf(storageConf);
    parser.setSchemaId(descriptor.getSchemaId());
    parser.setRowkeyLayout(layout);
    parser.setFamilyLayout(layoutDescriptors.get(nextFieldIndex++));
    parser.setQualifierLayout(layoutDescriptors.get(nextFieldIndex++));
    layout = layoutDescriptors.get(nextFieldIndex++);
    boolean hasTimestamp = HBaseCell.TIMESTAMP.equals(layout.fieldName());
    if (hasTimestamp) {
      parser.setTimestampLayout(layout);
      layout = layoutDescriptors.get(nextFieldIndex++);
    }
    parser.setValueLayout(layout);

    parser.setPrimary(descriptor.isPrimary());
    parser.setMode(descriptor.getMode());
    parser.setSchemaConf(descriptor.getConf());

    List<String> rowkeys = HBaseStorage.FIELDS.subList(1, 2);
    List<String> keys = HBaseStorage.FIELDS.subList(1, 4);
    List<String> fields
        = hasTimestamp ? HBaseStorage.FIELDS_WITH_TIMESTAMP.subList(1, 6) :
        HBaseStorage.FIELDS.subList(1, 5);

    SingleTableHBaseConnectionFactory connectionFactory = new SingleTableHBaseConnectionFactory(
        this, TableName.valueOf(NAMESPACE_PARAM.get(storageConf), table), rowkeys, keys, fields);
    return parser.build(connectionFactory);
  }

  @Override
  protected List<String> getKeys() {
    return KEY_FIELDS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HBaseStorage other = (HBaseStorage) o;
    return Objects.equals(other.conf, conf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(conf);
  }


  public static class Factory extends StorageFactory {
    @Override
    protected Storage doCreate(ValorConf config) {
      return new HBaseStorage(config);
    }

    @Override
    public Collection<String> getAliases() {
      return Arrays.asList(
          getName(),
          "jp.ameba.valor.hbase.storage.HBaseStorage"
      );
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Storage> getProvidedClass() {
      return HBaseStorage.class;
    }
  }
}
