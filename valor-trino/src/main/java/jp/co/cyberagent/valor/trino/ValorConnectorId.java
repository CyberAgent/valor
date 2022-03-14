package jp.co.cyberagent.valor.trino;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

/**
 *
 */
public class ValorConnectorId {

  private final String id;

  public ValorConnectorId(String id) {
    this.id = requireNonNull(id, "id is null");
  }

  @Override
  public String toString() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ValorConnectorId that = (ValorConnectorId) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
