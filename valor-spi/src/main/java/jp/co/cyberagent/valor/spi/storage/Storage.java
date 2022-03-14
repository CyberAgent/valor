package jp.co.cyberagent.valor.spi.storage;

import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;

public abstract class Storage {

  public abstract Schema buildSchema(Relation relation, SchemaDescriptor descriptor)
      throws ValorException;

  public abstract ValorConf getConf();

}
