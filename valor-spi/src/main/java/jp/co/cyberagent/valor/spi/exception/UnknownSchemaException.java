package jp.co.cyberagent.valor.spi.exception;

@SuppressWarnings("serial")
public class UnknownSchemaException extends ValorRuntimeException {

  public UnknownSchemaException(String relId, String schemaId) {
    super(String.format("Schema: %s.%s is unknown", relId, schemaId));
  }
}
