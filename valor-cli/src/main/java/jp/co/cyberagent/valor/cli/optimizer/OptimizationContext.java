package jp.co.cyberagent.valor.cli.optimizer;

import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.cli.ast.ScanNode;
import jp.co.cyberagent.valor.spi.optimize.DataStats;
import jp.co.cyberagent.valor.spi.relation.Relation;

public class OptimizationContext {

  private Map<String, ScanNode> queries;

  private List<Relation> relations;

  private Context context;

  public Map<String, ScanNode> getQueries() {
    return queries;
  }

  public void setQueries(Map<String, ScanNode> queries) {
    this.queries = queries;
  }

  public List<Relation> getRelations() {
    return relations;
  }

  public void setRelations(List<Relation> relations) {
    this.relations = relations;
  }

  public Context getContext() {
    return context;
  }

  public void setContext(Context context) {
    this.context = context;
  }

  public static class Context {

    private Map<String, Integer> queryWeight;
    private Map<String, DataStats> dataStats;
    private Map<String, String> storageConf;

    public Map<String, Integer> getQueryWeight() {
      return queryWeight;
    }

    public void setQueryWeight(Map<String, Integer> queryWeight) {
      this.queryWeight = queryWeight;
    }

    public Map<String, DataStats> getDataStats() {
      return dataStats;
    }

    public void setDataStats(
        Map<String, DataStats> dataStats) {
      this.dataStats = dataStats;
    }

    public Map<String, String> getStorageConf() {
      return storageConf;
    }

    public void setStorageConf(Map<String, String> storageConf) {
      this.storageConf = storageConf;
    }
  }
}
