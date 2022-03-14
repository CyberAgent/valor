package jp.co.cyberagent.valor.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.metadata.InMemorySchemaRepository;
import jp.co.cyberagent.valor.sdk.plan.function.EqualOperator;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.plan.model.AttributeNameExpression;
import jp.co.cyberagent.valor.spi.plan.model.ConstantExpression;
import jp.co.cyberagent.valor.spi.plan.model.Expression;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kohsuke.args4j.CmdLineParser;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestSearchCommand {
  @Captor
  ArgumentCaptor<RelationScan> queryCaptor;

  @Mock
  private ValorConnection client;

  private Relation relation = ImmutableRelation.builder().relationId("rel")
      .addAttribute("k", true, StringAttributeType.INSTANCE)
      .addAttribute("v", false, IntegerAttributeType.INSTANCE)
      .build();

  @BeforeEach
  public void initMock() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testExecute() throws Exception {
    ValorContext context = StandardContextFactory.create();
    SchemaRepository repository = new InMemorySchemaRepository(context.getConf());
    repository.init(context);
    repository.createRelation(relation, true);

    when(client.getSchemaRepository()).thenReturn(repository);
    SearchCommand testCommand = new SearchCommand();
    CmdLineParser parser = new CmdLineParser(testCommand);
    // content of testSearch.json
    // { "type": "equal", "key": "k", "value": "v"}
    String[] args = {"-f", "src/test/resources/testSearch.json", "-r", "rel"};
    parser.parseArgument(args);

    testCommand.execute(client);
    verify(client, times(1)).scan(queryCaptor.capture());

    RelationScan q = queryCaptor.getAllValues().get(0);
    assertEquals("rel", q.getFrom().getRelation().getRelationId());

    Expression capturedExpression = q.getCondition();
    assertThat(capturedExpression, instanceOf(EqualOperator.class));
    EqualOperator eop = (EqualOperator) capturedExpression;
    assertEquals("k", ((AttributeNameExpression)eop.getLeft()).getName());
    assertEquals("v", ((ConstantExpression)eop.getRight()).getValue());

  }
}
