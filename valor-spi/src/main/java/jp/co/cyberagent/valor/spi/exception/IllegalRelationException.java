package jp.co.cyberagent.valor.spi.exception;

@SuppressWarnings("serial")
public class IllegalRelationException extends ValorException {
  public IllegalRelationException(String relId, String reason) {
    super(String.format("Relation: %s is invalid: %s", relId, reason));
  }
}
