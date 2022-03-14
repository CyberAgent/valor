package jp.co.cyberagent.valor.hbase.storage.snapshot;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import jp.co.cyberagent.valor.hbase.storage.CellSchemaBuilder;
import jp.co.cyberagent.valor.hbase.storage.HBaseCell;
import jp.co.cyberagent.valor.sdk.storage.kv.KeyValueStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageFactory;


public class HBaseSnapshotStorage extends KeyValueStorage implements AutoCloseable {

  public static final String NAME = "hbaseSnapshot";

  public static final String SNAPSHOT_NAME_KEY = "valor.hbase.snapshot.name";

  public static final List<String> FIELDS_WITH_TIMESTAMP = Arrays.asList(
      HBaseCell.ROWKEY, HBaseCell.FAMILY, HBaseCell.QUALIFIER,
      HBaseCell.TIMESTAMP, HBaseCell.VALUE);

  public static final List<String> FIELDS = Arrays.asList(
      HBaseCell.ROWKEY, HBaseCell.FAMILY, HBaseCell.QUALIFIER, HBaseCell.VALUE);

  public static final List<String> KEY_FIELDS = Arrays.asList(
      HBaseCell.ROWKEY, HBaseCell.FAMILY, HBaseCell.QUALIFIER);

  public HBaseSnapshotStorage(ValorConf conf) {
    super(conf);
  }

  @Override
  protected StorageConnectionFactory getConnectionFactory(Relation relation,
                                                          SchemaDescriptor descriptor) {
    throw new UnsupportedOperationException("should not be called");
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public Schema buildSchema(Relation relation, SchemaDescriptor descriptor) throws ValorException {
    List<FieldLayout> layoutDescriptors = descriptor.getFields();

    ValorConf storageConf = descriptor.getStorageConf();
    int nextFieldIndex = 0;

    CellSchemaBuilder parser = new CellSchemaBuilder();
    parser.setRelation(relation);
    parser.setStorageClass(getClass().getCanonicalName());
    parser.setStorageConf(storageConf);
    parser.setSchemaId(descriptor.getSchemaId());
    parser.setRowkeyLayout(layoutDescriptors.get(nextFieldIndex++));
    parser.setFamilyLayout(layoutDescriptors.get(nextFieldIndex++));
    parser.setQualifierLayout(layoutDescriptors.get(nextFieldIndex++));
    FieldLayout layout = layoutDescriptors.get(nextFieldIndex++);
    boolean hasTimestamp = HBaseCell.TIMESTAMP.equals(layout.fieldName());
    if (hasTimestamp) {
      parser.setTimestampLayout(layout);
      layout = layoutDescriptors.get(nextFieldIndex++);
    }
    parser.setValueLayout(layout);

    parser.setPrimary(descriptor.isPrimary());
    parser.setMode(descriptor.getMode());
    parser.setSchemaConf(descriptor.getConf());

    List<String> fields = hasTimestamp ? FIELDS_WITH_TIMESTAMP : FIELDS;
    String snapshotName = conf.get(SNAPSHOT_NAME_KEY);

    SnapshotConnectionFactory connectionFactory = new SnapshotConnectionFactory(
        this, snapshotName, fields);
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
    HBaseSnapshotStorage other = (HBaseSnapshotStorage) o;
    return Objects.equals(other.conf, conf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(conf);
  }


  public static class Factory extends StorageFactory {
    @Override
    protected Storage doCreate(ValorConf config) {
      return new HBaseSnapshotStorage(config);
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends Storage> getProvidedClass() {
      return HBaseSnapshotStorage.class;
    }
  }
}
