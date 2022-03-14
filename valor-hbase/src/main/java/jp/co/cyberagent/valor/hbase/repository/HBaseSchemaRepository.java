package jp.co.cyberagent.valor.hbase.repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import jp.co.cyberagent.valor.hbase.exception.InvalidRowException;
import jp.co.cyberagent.valor.hbase.util.HBaseUtil;
import jp.co.cyberagent.valor.sdk.metadata.SchemaRepositoryBase;
import jp.co.cyberagent.valor.spi.ThreadContextClassLoader;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfParam;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class HBaseSchemaRepository extends SchemaRepositoryBase {
  public static final String NAME = "hbase";

  public static final ValorConfParam SCHEMA_TABLE = new ValorConfParam("valor.hbase.metadata.table",
      "valor_schema");
  public static final ValorConfParam SCHEMA_FAMILY = new ValorConfParam(
      "valor.hbase.metadata.family", "s");

  public static final byte[] EMPTY_BYTES = new byte[0];
  private final Connection connection;
  private final Table table;
  private final byte[] family;

  protected HBaseSchemaRepository(ValorConf conf) {
    super(conf);
    TableName tableName = TableName.valueOf(SCHEMA_TABLE.get(conf));
    family = ByteUtils.toBytes(SCHEMA_FAMILY.get(conf));
    // set context classloader for ServiceLoader in Hadoop (e.g. FileSystem)
    try (ThreadContextClassLoader dummy =
        new ThreadContextClassLoader(getClass().getClassLoader())) {
      this.connection =
          ConnectionFactory.createConnection(HBaseUtil.toConfiguration(conf));
      this.table = connection.getTable(tableName);
    } catch (IOException e) {
      throw new ValorRuntimeException(e.getMessage(), e);
    }
  }

  @Override
  protected Collection<String> doGetRelationIds() throws ValorException {
    List<String> relations = new ArrayList<>();
    Scan scan = new Scan();
    scan.setFilter(new FirstKeyOnlyFilter());
    scan.addFamily(family);
    try (ResultScanner scanner = table.getScanner(scan)) {
      Result r;
      while ((r = scanner.next()) != null) {
        Cell cell = r.listCells().get(0);
        relations.add(ByteUtils.toString(CellUtil.cloneRow(cell)));
      }
    } catch (IOException e) {
      throw new ValorException(e);
    }
    return relations;
  }

  @Override
  protected Relation doGetRelation(String relId) throws ValorException {
    Get get = new Get(ByteUtils.toBytes(relId));
    get.addColumn(family, EMPTY_BYTES);
    try {
      Result result = table.get(get);
      if (result.isEmpty()) {
        return null;
      }
      byte[] value = CellUtil.cloneValue(result.listCells().get(0));
      return serde.readRelation(relId, value);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected void doRegisterRelation(Relation relation) throws ValorException {
    byte[] value = serde.serialize(relation);
    Put put = new Put(ByteUtils.toBytes(relation.getRelationId()));
    put.addColumn(family, EMPTY_BYTES, value);
    try {
      table.put(put);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected String doDropRelation(String relId) throws ValorException {
    Delete del = new Delete(ByteUtils.toBytes(relId));
    try {
      table.delete(del);
    } catch (IOException e) {
      throw new ValorException(e);
    }
    return relId;
  }

  @Override
  protected Collection<Schema> doGetSchemas(String relationId) throws ValorException {
    byte[] rowkey = ByteUtils.toBytes(relationId);
    Get get = new Get(rowkey);
    get.addFamily(family);
    Relation relation = null;
    Collection<SchemaDescriptor> sds = new ArrayList<>();
    try {
      Result result = table.get(get);
      for (Cell cell : result.listCells()) {
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        if (qualifier.length == 0) {
          relation = serde.readRelation(relationId, CellUtil.cloneValue(cell));
        } else {
          SchemaDescriptor sd
              = serde.readSchema(ByteUtils.toString(qualifier), CellUtil.cloneValue(cell));
          sds.add(sd);
        }
      }
    } catch (IOException e) {
      throw new ValorException(e);
    }
    if (relation == null) {
      throw new InvalidRowException(rowkey, "relation definition not found");
    }
    Collection<Schema> schemas = new ArrayList<>(sds.size());
    for (SchemaDescriptor sd : sds) {
      schemas.add(context.buildSchmea(relation, sd));
    }
    return schemas;
  }

  @Override
  protected Schema doGetSchema(String relationId, String schemaId) throws ValorException {
    byte[] rowkey = ByteUtils.toBytes(relationId);
    byte[] schemaQualfieir = ByteUtils.toBytes(schemaId);
    Get get = new Get(rowkey);
    get.addColumn(family, EMPTY_BYTES);
    get.addColumn(family, schemaQualfieir);
    Relation relation = null;
    byte[] serializedSchema = null;
    try {
      Result result = table.get(get);
      for (Cell cell : result.listCells()) {
        byte[] qualifier = CellUtil.cloneQualifier(cell);
        if (qualifier.length == 0) {
          relation = serde.readRelation(relationId, CellUtil.cloneValue(cell));
        } else if (ByteUtils.equals(qualifier, schemaQualfieir)) {
          serializedSchema = CellUtil.cloneValue(cell);
        }
      }
    } catch (IOException e) {
      throw new ValorException(e);
    }
    if (relation == null) {
      throw new InvalidRowException(rowkey, "relation definition not found");
    }
    if (serializedSchema == null) {
      return null;
    }

    SchemaDescriptor schemaDescriptor = serde.readSchema(relationId, serializedSchema);
    return context.buildSchmea(relation, schemaDescriptor);
  }

  @Override
  protected void doCreateSchema(String relId, SchemaDescriptor schema) throws ValorException {
    byte[] rowkey = ByteUtils.toBytes(relId);
    byte[] qualifier = ByteUtils.toBytes(schema.getSchemaId());
    byte[] value = serde.serialize(schema);
    Put put = new Put(rowkey);
    put.addColumn(family, qualifier, value);
    try {
      table.put(put);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected String doDropSchema(String relId, String schemaId) throws ValorException {
    byte[] rowkey = ByteUtils.toBytes(relId);
    byte[] qualifier = ByteUtils.toBytes(schemaId);
    Delete del = new Delete(rowkey);
    del.addColumns(family, qualifier);
    try {
      table.delete(del);
    } catch (IOException e) {
      throw new ValorException(e);
    }
    return schemaId;
  }

  @Override
  public void close() throws IOException {
    try {
      table.close();
    } finally {
      connection.close();
    }
  }

  public static class Factory implements SchemaRepositoryFactory {
    @Override
    public SchemaRepository create(ValorConf conf) {
      return new HBaseSchemaRepository(conf);
    }

    @Override
    public Collection<String> getAliases() {
      return Arrays.asList(
          getName(),
          "jp.ameba.valor.hbase.repository.HBaseSchemaRepository"
      );
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends SchemaRepository> getProvidedClass() {
      return HBaseSchemaRepository.class;
    }
  }
}
