package jp.co.cyberagent.valor.spi.optimize;

import java.util.Map;

public class DataStats {
  private Map<String, Double> cardinality;
  private Map<String, Double> size;

  public Map<String, Double> getCardinality() {
    return cardinality;
  }

  public void setCardinality(Map<String, Double> cardinality) {
    this.cardinality = cardinality;
  }

  public Map<String, Double> getSize() {
    return size;
  }

  public void setSize(Map<String, Double> size) {
    this.size = size;
  }
}
