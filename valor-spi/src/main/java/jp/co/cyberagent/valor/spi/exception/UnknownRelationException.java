package jp.co.cyberagent.valor.spi.exception;

@SuppressWarnings("serial")
public class UnknownRelationException extends InvalidOperationException {

  public UnknownRelationException(String relId) {
    super("Relation: " + relId + " is unknown");
  }
}
