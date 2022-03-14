package jp.co.cyberagent.valor.sdk.storage.kv;

import static jp.co.cyberagent.valor.spi.storage.StorageScan.UNSINGED_BYTES_COMPARATOR;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import jp.co.cyberagent.valor.sdk.formatter.CumulativeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.DisassembleFormatter;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedCumulativeKeyValuesSchema;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedMonoKeyValueSchema;
import jp.co.cyberagent.valor.sdk.schema.kv.SortedMultiKeyValuesSchema;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.IllegalSchemaException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;
import jp.co.cyberagent.valor.spi.storage.Storage;
import jp.co.cyberagent.valor.spi.storage.StorageConnection;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;
import jp.co.cyberagent.valor.spi.storage.StorageFactory;

/**
 * in memory storage for test
 */
public class EmbeddedEKVStorage extends KeyValueStorage {

  public static final String KEY = "key";
  public static final String COL = "col";
  public static final String VAL = "val";

  public static final List<String> FIELDS = Arrays.asList(KEY, COL, VAL);

  private NavigableMap<byte[], NavigableMap<byte[], byte[]>> values =
      new TreeMap<>(UNSINGED_BYTES_COMPARATOR);

  public EmbeddedEKVStorage(ValorConf conf) {
    super(conf);
  }

  @Override
  public Schema buildSchema(Relation relation, SchemaDescriptor descriptor) throws ValorException {
    boolean multiRow = false;
    boolean multiCell = false;
    boolean cumulative = false;
    List<FieldLayout> fields = descriptor.getFields();

    for (int i = 0; i < fields.size(); i++) {
      FieldLayout field = fields.get(i);
      for (Segment formatter : field.formatters()) {
        Segment elm = formatter.getFormatter();
        boolean splitted = elm instanceof DisassembleFormatter;
        cumulative = cumulative || elm instanceof CumulativeValueFormatter;
        if (i < getRowkeys().size()) {
          multiRow = multiRow || splitted;
        }
        if (i < getKeys().size()) {
          multiCell = multiCell || splitted;
          if (cumulative) {
            throw new IllegalSchemaException(relation.getRelationId(), descriptor.getSchemaId(),
                "cumulative element is not allowed in key");
          }
        }
      }
    }

    if (multiRow) {
      throw new UnsupportedOperationException("multirow schema is not supported currently");
    }

    StorageConnectionFactory connectionFactory = getConnectionFactory(relation, descriptor);
    if (multiCell) {
      return cumulative ? new SortedCumulativeKeyValuesSchema(connectionFactory, relation,
          descriptor) :
          new SortedMultiKeyValuesSchema(connectionFactory, relation, descriptor);
    } else {
      return new SortedMonoKeyValueSchema(connectionFactory, relation, descriptor);
    }
  }


  protected List<String> getRowkeys() {
    return Arrays.asList(KEY);
  }

  @Override
  protected List<String> getKeys() {
    return Arrays.asList(KEY, COL);
  }

  @Override
  protected StorageConnectionFactory getConnectionFactory(Relation relation,
                                                          SchemaDescriptor descriptor) {
    return new KeyValueStorageConnectionFactory(EmbeddedEKVStorage.this) {
      @Override
      public StorageConnection connect() throws ValorException {
        return new EmbeddedEKVStorageConnection(EmbeddedEKVStorage.this);
      }

      @Override
      public List<String> getRowkeyFields() {
        return Arrays.asList(KEY);
      }

      @Override
      public List<String> getKeyFields() {
        return Arrays.asList(KEY, COL);
      }

      @Override
      public List<String> getFields() {
        return Arrays.asList(KEY, COL, VAL);
      }

      @Override
      public boolean equals(Object o) {
        return false;
      }

      @Override
      public int hashCode() {
        return EmbeddedEKVStorage.this.hashCode();
      }
    };
  }

  public NavigableMap<byte[], byte[]> get(byte[] key) {
    return values.get(key);
  }

  public void put(byte[] key, NavigableMap<byte[], byte[]> val) {
    values.put(key, val);
  }

  public void put(byte[] key, byte[] col, byte[] val) {
    NavigableMap<byte[], byte[]> m = values.get(key);
    if (m == null) {
      m = new TreeMap<>(UNSINGED_BYTES_COMPARATOR);
      values.put(key, m);
    }
    m.put(col, val);
  }

  public void remove(byte[] key) {
    values.remove(key);
  }

  public Iterator<byte[]> keyIterator() {
    return values.keySet().iterator();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EmbeddedEKVStorage other = (EmbeddedEKVStorage)o;
    return Objects.equals(other.conf, conf);
  }

  @Override
  public int hashCode() {
    return Objects.hash(conf);
  }

  public static class Factory extends StorageFactory {
    @Override
    protected Storage doCreate(ValorConf config) {
      return new EmbeddedEKVStorage(config);
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public Class<? extends Storage> getProvidedClass() {
      return EmbeddedEKVStorage.class;
    }
  }
}
