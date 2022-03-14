package jp.co.cyberagent.valor.cli.ast;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.IOException;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.sdk.plan.function.GreaterthanOperator;
import jp.co.cyberagent.valor.sdk.plan.function.LessthanOperator;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.model.AndOperator;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;
import jp.co.cyberagent.valor.spi.plan.model.NotOperator;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class TestSemanticAnalyzer {

  private final static String REL_ID = "r";
  private final static String KEY1 = "k1";
  private final static String KEY2 = "k2";
  private final static String VAL1 = "v1";
  private final static String VAL2 = "v2";

  private static ValorContext context;
  private static SchemaRepository repository;
  private static ObjectMapper mapper;

  @BeforeAll
  public static void init() throws ValorException {
    context = StandardContextFactory.create(new ValorConfImpl());
    repository = new InMemorySchemaRepository(context.getConf());
    Relation relation = ImmutableRelation.builder().relationId(REL_ID)
        .addAttribute(KEY1, true, StringAttributeType.INSTANCE)
        .addAttribute(KEY2, true, StringAttributeType.INSTANCE)
        .addAttribute(VAL1, false, StringAttributeType.INSTANCE)
        .addAttribute(VAL2, false, StringAttributeType.INSTANCE)
        .build();
    repository.createRelation(relation, false);


    Module module = new SimpleModule()
        .addDeserializer(ExpressionNode.class, new AstNodeDeserializer.ExpressionDeserializer());
    mapper = new ObjectMapper().registerModule(module);

  }

  @AfterAll
  public static void tearDown() throws IOException {
    repository.close();
  }

  @Test
  public void testNestedAndOperators() throws Exception {
    SemanticAnalyzer analyzer = new SemanticAnalyzer(repository);
    String exp = "{" +
        "  \"type\":\"and\"," +
        "  \"queries\":[" +
        "    {\"key\":\"k1\",\"type\":\"greaterthan\",\"value\":\"a\"}," +
        "    {\"key\":\"k1\",\"type\":\"lessthan\",\"value\":\"z\"}," +
        "    {" +
        "      \"type\":\"and\"," +
        "      \"queries\":[" +
        "        {\"key\":\"k2\",\"type\":\"equal\",\"value\":\"x\"}" +
        "      ]" +
        "    }" +
        "  ]" +
        "}";
    ExpressionNode ast = mapper.readValue(exp, ExpressionNode.class);
    ScanNode queryNode = new ScanNode(
        new ProjectionClauseNode(new AllItemNode()),
        new RelationSourceNode(REL_ID),
        new WhereClauseNode(ast));
    RelationScan logicalNode = (RelationScan) analyzer.walk(queryNode);
    AndOperator aop = new AndOperator();
    aop.addOperand(new EqualOperator(KEY2, StringAttributeType.INSTANCE, "x"));
    LogicalPlanNode expected = AndOperator.join(
        new GreaterthanOperator(KEY1, StringAttributeType.INSTANCE, "a"),
        new LessthanOperator(KEY1, StringAttributeType.INSTANCE, "z"),
        aop
    );
    assertEquals(expected, logicalNode.getCondition());
  }

  @Test
  public void testNotOperator() throws Exception {
    SemanticAnalyzer analyzer = new SemanticAnalyzer(repository);
    String exp = "{" +
      "  \"type\":\"not\"," +
      "  \"query\":{" +
      "    \"key\":\"k2\",\"type\":\"equal\",\"value\":\"x\"" +
      "  }" +
      "}";
    ExpressionNode ast = mapper.readValue(exp, ExpressionNode.class);
    ScanNode queryNode = new ScanNode(
      new ProjectionClauseNode(new AllItemNode()),
      new RelationSourceNode(REL_ID),
      new WhereClauseNode(ast));
    RelationScan logicalNode = (RelationScan) analyzer.walk(queryNode);
    NotOperator expected = new NotOperator(
      new EqualOperator(KEY2, StringAttributeType.INSTANCE, "x")
    );
    assertEquals(expected, logicalNode.getCondition());
  }

  @Test
  public void testConjunctionOfTwoNegations() throws Exception {
    SemanticAnalyzer analyzer = new SemanticAnalyzer(repository);
    String exp = "{" +
      "  \"type\":\"and\"," +
      "  \"queries\":[" +
      "    {" +
      "      \"type\":\"not\"," +
      "      \"query\":{" +
      "        \"key\":\"k1\",\"type\":\"equal\",\"value\":\"v1\"" +
      "      }" +
      "    }," +
      "    {" +
      "      \"type\":\"not\"," +
      "      \"query\":{" +
      "        \"type\":\"and\"," +
      "        \"queries\":[" +
      "          {" +
      "           \"key\":\"k2\",\"type\":\"equal\",\"value\":\"v2\"" +
      "          }" +
      "        ]" +
      "      }" +
      "    }" +
      "  ]" +
      "}";
    ExpressionNode ast = mapper.readValue(exp, ExpressionNode.class);
    ScanNode queryNode = new ScanNode(
      new ProjectionClauseNode(new AllItemNode()),
      new RelationSourceNode(REL_ID),
      new WhereClauseNode(ast));
    RelationScan logicalNode = (RelationScan) analyzer.walk(queryNode);

    AndOperator aop = new AndOperator();
    aop.addOperand(new EqualOperator(KEY2, StringAttributeType.INSTANCE, "v2"));
    EqualOperator eop = new EqualOperator(KEY1, StringAttributeType.INSTANCE, "v1");
    LogicalPlanNode expected = AndOperator.join(
      new NotOperator(eop),
      new NotOperator(aop)
    );
    assertEquals(expected, logicalNode.getCondition());
  }

}
