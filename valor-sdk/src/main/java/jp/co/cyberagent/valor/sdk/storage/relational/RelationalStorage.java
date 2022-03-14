package jp.co.cyberagent.valor.sdk.storage.relational;

import jp.co.cyberagent.valor.sdk.schema.relational.RelationalSchema;
import jp.co.cyberagent.valor.sdk.storage.StorageBase;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.StorageConnectionFactory;

public abstract class RelationalStorage extends StorageBase {

  public RelationalStorage(ValorConf conf) {
    super(conf);
  }

  @Override
  public Schema buildSchema(Relation relation, SchemaDescriptor schemaDescriptor) {
    return new RelationalSchema(getConnectionFactory(relation, schemaDescriptor), relation,
        schemaDescriptor);
  }

  protected abstract StorageConnectionFactory getConnectionFactory(
      Relation relation,
      SchemaDescriptor schemaDescriptor);
}
