package jp.co.cyberagent.valor.spi.exception;

import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;

@SuppressWarnings("serial")
public class DisabledOperationException extends InvalidOperationException {
  public DisabledOperationException(String tupleId, String schemaId,
                                    Relation.OperationType opType) {
    super(String.format("%s is disabled in %s.%s", opType.name(), tupleId, schemaId));
  }

  public DisabledOperationException(Schema def, Relation.OperationType opType) {
    super(String.format("%s is disabled in %s", opType.name(), def.getSchemaId()));
  }
}
