package jp.co.cyberagent.valor.hbase.optimize;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.optimize.HeuristicCostSolver;
import jp.co.cyberagent.valor.sdk.optimize.HeuristicSpaceSolver;
import jp.co.cyberagent.valor.sdk.plan.SimplePlan;
import jp.co.cyberagent.valor.sdk.plan.StaticCostBasedPrimitivePlanner;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.optimize.DataStats;
import jp.co.cyberagent.valor.spi.optimize.Enumerator;
import jp.co.cyberagent.valor.spi.optimize.EvaluatedPlan;
import jp.co.cyberagent.valor.spi.plan.Planner;
import jp.co.cyberagent.valor.spi.plan.ScanPlan;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.ProjectionClause;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.plan.model.RelationSource;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestHBaseOptimizer {

  private static final String Q_ID = "Q_id";
  private static final String Q_ID_NAME = "Q_id_name";

  private static Relation relation;
  private static RelationScan queryIdPred;
  private static RelationScan queryIdNamePred;
  private static SchemaDescriptor expectedSchemaIdName;
  private static SchemaDescriptor expectedSchemaIdDate;

  private static ValorConf conf;
  private static Map<String, RelationScan> queries = new HashMap<>();
  private static final String idAttribute = "id";
  private static final String nameAttribute = "name";
  private static final String dateAttribute = "date";
  private static final String RELATION_ID = "r";

  @Mock
  private SchemaRepository repository;

  @BeforeAll
  public static void setup() throws ValorException {
    relation = ImmutableRelation.builder().relationId(RELATION_ID)
        .addAttribute(idAttribute, true, IntegerAttributeType.INSTANCE)
        .addAttribute(nameAttribute, false, StringAttributeType.INSTANCE)
        .addAttribute(dateAttribute, false, StringAttributeType.INSTANCE)
        .build();

    AttributeNameExpression idExp
        = new AttributeNameExpression(idAttribute, IntegerAttributeType.INSTANCE);
    AttributeNameExpression nameExp
        = new AttributeNameExpression(nameAttribute, StringAttributeType.INSTANCE);
    AttributeNameExpression dateExp
        = new AttributeNameExpression(dateAttribute, StringAttributeType.INSTANCE);

    EqualOperator idPred = new EqualOperator(idExp, new ConstantExpression(0));
    EqualOperator namePred = new EqualOperator(nameExp, new ConstantExpression(""));

    ProjectionClause select = new ProjectionClause(idExp, nameExp, dateExp);
    RelationSource from = new RelationSource(relation);

    queryIdPred = new RelationScan(select, from, idPred);
    queries.put(Q_ID, queryIdPred);

    queryIdNamePred = new RelationScan(select, from, AndOperator.join(idPred, namePred));
    queries.put(Q_ID_NAME, queryIdNamePred);

    List<String> rowkey = new ArrayList<>();
    rowkey.add(idAttribute);
    rowkey.add(nameAttribute);
    conf = new ValorConfImpl();
    HBaseEnumerator enumerator = new HBaseEnumerator();
    expectedSchemaIdName = enumerator
        .createSchemaFromAttributes(relation, Arrays.asList(idPred), queryIdPred, conf);

    rowkey = new ArrayList<>();
    rowkey.add(idAttribute);
    rowkey.add(dateAttribute);
    expectedSchemaIdDate = enumerator
        .createSchemaFromAttributes(relation, Arrays.asList(idPred, namePred)
            , queryIdNamePred, conf);
  }

  @BeforeEach
  public void initMock() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void TestEnumerator() {
    Enumerator enumerator = new HBaseEnumerator();
    List<SchemaDescriptor> schemas = enumerator.enumerateSchemas(queries.values(), conf);

    assertTrue(schemas.stream().anyMatch(s -> isSameKV(s, expectedSchemaIdName)));
    assertTrue(schemas.stream().anyMatch(s -> isSameKV(s, expectedSchemaIdDate)));
  }

  // TODO make HBase independent optimizer
  @Test
  @Disabled
  public void TestOptimizer() throws ValorException {

    DataStats dataStats = new DataStats();
    dataStats.setCardinality(new HashMap<String, Double>(){
      {
        put(idAttribute, 100.0);
        put(nameAttribute, 1000.0);
        put(dateAttribute, 10.0);
      }
    });

    Map<String, DataStats> relStats = new HashMap<String, DataStats>(){
      {
        put(RELATION_ID, dataStats);
      }
    };

    Planner queryPlanner = new StaticCostBasedPrimitivePlanner(relStats);

    Map<String, Collection<ScanPlan>> plans = new HashMap<>();
    plans.put(Q_ID, queryPlanner.enumerateAllPlans(queries.get(Q_ID), repository));
    plans.put(Q_ID_NAME, queryPlanner.enumerateAllPlans(queries.get(Q_ID_NAME), repository));

    Map<String, Collection<EvaluatedPlan>> plansWithCost = new HashMap<>();
    for (Map.Entry<String, Collection<ScanPlan>> q : plans.entrySet()) {
      Collection<EvaluatedPlan> evaluatedPlans = new ArrayList<>(queries.size());
      for (ScanPlan plan : q.getValue()) {
        double cost = queryPlanner.evaluate(plan);
        evaluatedPlans.add(new EvaluatedPlan(plan, cost));
      }
      plansWithCost.put(q.getKey(), evaluatedPlans);
    }

    // heuristic method for cost objective does not care about sharing schemas
    // and gives schemas that has the minimum cost for each query .
    Map<String, ScanPlan> queryPlans = (new HeuristicCostSolver()).solve(plansWithCost);
    for (Map.Entry<String, ScanPlan> solvedPlan : queryPlans.entrySet()) {
      Collection<ScanPlan> allPlans = plans.get(solvedPlan.getKey());
      double solvedCost = queryPlanner.evaluate(solvedPlan.getValue());
      for (ScanPlan p : allPlans ) {
        double c = queryPlanner.evaluate(p);
        assertTrue(c >= solvedCost);
      }
    }

    // heuristic method for size objective first minimize the # of schemas,
    // and then gives the schema-pairs that have minimum cost
    queryPlans = (new HeuristicSpaceSolver()).solve(plansWithCost);
    SimplePlan spId = (SimplePlan) queryPlans.get(Q_ID);
    SimplePlan spIdName = (SimplePlan) queryPlans.get(Q_ID_NAME);
    assertThat(spId.getScan().getSchema(), equalTo(spIdName.getScan().getSchema()));
  }

  private boolean isSameKV(SchemaDescriptor s1, SchemaDescriptor s2) {
    List<String> k1 = Enumerator.getAttrNamesByLayout("rowkey", s1);
    List<String> k2 = Enumerator.getAttrNamesByLayout("rowkey", s2);

    List<String> v1 = Enumerator.getAttrNamesByLayout("value", s1);
    List<String> v2 = Enumerator.getAttrNamesByLayout("value", s2);

    return k1.equals(k2) && v1.equals(v2);
  }
}
