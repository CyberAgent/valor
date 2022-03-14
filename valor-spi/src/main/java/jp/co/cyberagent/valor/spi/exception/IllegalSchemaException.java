package jp.co.cyberagent.valor.spi.exception;

@SuppressWarnings("serial")
public class IllegalSchemaException extends ValorException {
  public IllegalSchemaException(String relId, String schemaId, String reason) {
    super(String.format("Schema: %s.%s is invalid: %s", relId, schemaId, reason));
  }
}
