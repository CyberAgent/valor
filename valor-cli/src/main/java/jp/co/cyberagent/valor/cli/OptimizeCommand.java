package jp.co.cyberagent.valor.cli;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.cli.ast.AstNodeDeserializer;
import jp.co.cyberagent.valor.cli.ast.ScanNode;
import jp.co.cyberagent.valor.cli.ast.SemanticAnalyzer;
import jp.co.cyberagent.valor.cli.optimizer.OptimizationContext;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.sdk.metadata.RelationDeserializer;
import jp.co.cyberagent.valor.sdk.plan.SimplePlan;
import jp.co.cyberagent.valor.sdk.plan.StaticCostBasedPrimitivePlanner;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.MetadataSerde;
import jp.co.cyberagent.valor.spi.optimize.DataStats;
import jp.co.cyberagent.valor.spi.optimize.EvaluatedPlan;
import jp.co.cyberagent.valor.spi.optimize.Optimizer;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.kohsuke.args4j.Option;

public class OptimizeCommand implements ClientCommand {

  @Option(name = "-f", usage = "query file (json)", required = true)
  private String pathToQueryFile;

  @Option(name = "-s", usage = "solver name")
  private String solverName = "jp.co.cyberagent.valor.sdk.optimize.HeuristicSpaceSolver";

  @Option(name = "-v", usage = "verbose")
  private boolean verbose = false;

  @Override
  public int execute(ValorConnection client) throws Exception {
    OptimizationContext optimizationContext = readOptimizationContext(pathToQueryFile, client);
    Map<String, ScanPlan> plans = optimize(client.getContext(), optimizationContext);

    Set<SchemaDescriptor> schemas = plans.values().stream().map(p -> {
      if (!(p instanceof SimplePlan)) {
        throw new UnsupportedOperationException("unsupported plan " + p);
      }
      SimplePlan sp = (SimplePlan) p;
      return SchemaDescriptor.from(sp.getScan().getSchema());
    }).collect(Collectors.toSet());

    MetadataJsonSerde serde = new MetadataJsonSerde(client.getContext());
    if (verbose) {
      describePlans(plans);
    }
    describe(serde, schemas);
    return 0;
  }

  public OptimizationContext readOptimizationContext(String queryFile, ValorConnection conn)
      throws ValorException {
    ObjectMapper om = new ObjectMapper()
        .registerModule(new SimpleModule()
            .addDeserializer(ScanNode.class, new AstNodeDeserializer.QueryDeserializer())
            .addDeserializer(Relation.class, new RelationDeserializer(null, conn.getContext()))
        );
    try {
      return om.readValue(new File(queryFile), OptimizationContext.class);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  public Map<String, ScanPlan> optimize(ValorContext context, OptimizationContext optimizeContext)
      throws ValorException {
    final InMemorySchemaRepository repository = new InMemorySchemaRepository(context.getConf());
    repository.init(context);
    List<Relation> relations = optimizeContext.getRelations();
    if (relations.size() != 1) {
      throw new IllegalArgumentException(
          "currently, only one relations is supported by optimize command");
    }
    Relation relation = relations.get(0);
    repository.createRelation(relation, false);

    final SemanticAnalyzer analyzer = new SemanticAnalyzer(repository);
    final Map<String, RelationScan> queries = new HashMap<>();
    for (Map.Entry<String, ScanNode> qn : optimizeContext.getQueries().entrySet()) {
      RelationScan query = (RelationScan) analyzer.walk(qn.getValue());
      queries.put(qn.getKey(), query);
    }

    // enumerate schemas and register them to a temporal repository
    final ValorConf storageConf = new ValorConfImpl(optimizeContext.getContext().getStorageConf());
    // TODO make optimizer configurable
    final Optimizer optimizer = context.createOptimizer("HBaseOptimizer", new HashMap<>());
    final List<SchemaDescriptor> schemas = optimizer.getEnumerator()
        .enumerateSchemas(queries.values(), storageConf);
    for (SchemaDescriptor schema : schemas) {
      repository.createSchema(relation.getRelationId(), schema, true);
    }

    // evaluate possible query plans
    final Map<String, DataStats> dataStats = optimizeContext.getContext().getDataStats();
    // TODO make planner configurable
    final Planner planner = new StaticCostBasedPrimitivePlanner(dataStats);
    final Map<String, Collection<EvaluatedPlan>> plans = new HashMap<>();
    for (Map.Entry<String, RelationScan> q : queries.entrySet()) {
      Collection<ScanPlan> queryPlans = planner.enumerateAllPlans(q.getValue(), repository);
      Collection<EvaluatedPlan> evaluatedPlans = new ArrayList<>(queries.size());
      for (ScanPlan plan : queryPlans) {
        double cost = planner.evaluate(plan);
        evaluatedPlans.add(new EvaluatedPlan(plan, cost));
      }
      plans.put(q.getKey(), evaluatedPlans);
    }

    Map<String, ScanPlan> queryPlans = optimizer.getSolvers().stream()
        .filter(s -> s.getName().equals(solverName))
        .findFirst().get()
        .solve(plans);

    return queryPlans;
  }

  private void describePlans(Map<String, ScanPlan> queryPlans) throws ValorException {
    for (Map.Entry<String, ScanPlan> e : queryPlans.entrySet()) {
      System.out.println("===== " + e.getKey() + " =====");
      ScanPlan p = e.getValue();
      if (!(p instanceof SimplePlan)) {
        throw new UnsupportedOperationException("unsupported plan " + p);
      }
      SimplePlan sp = (SimplePlan) p;
      System.out.println("schema: " + sp.getScan().getSchema().getSchemaId());
    }
  }

  private void describe(MetadataSerde serde, Set<SchemaDescriptor> schemas) throws ValorException {
    ObjectMapper mapper = new ObjectMapper();
    for (SchemaDescriptor s : schemas) {
      System.out.println("===== " + s.getSchemaId() + " =====");
      String schemaString = new String(serde.serialize(s));
      try {
        Object jsonObject = mapper.readValue(schemaString, Object.class);
        System.out.println(mapper.writerWithDefaultPrettyPrinter()
            .writeValueAsString(jsonObject));
      } catch (JsonProcessingException jpe) {
        throw new ValorException(jpe);
      }
    }
  }
  
  public String getPathToQueryFile() {
    return pathToQueryFile;
  }

  public String getSolverName() {
    return solverName;
  }

}
