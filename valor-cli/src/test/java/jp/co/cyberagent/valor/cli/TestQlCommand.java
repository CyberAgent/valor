package jp.co.cyberagent.valor.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.Collection;
import java.util.Collections;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedKVDefaultSchemaHandler;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.plan.TupleScanner;
import jp.co.cyberagent.valor.spi.plan.model.RelationScan;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.Test;
import org.kohsuke.args4j.CmdLineParser;
import org.mockito.ArgumentCaptor;

public class TestQlCommand {

  private Relation relation = ImmutableRelation.builder().relationId("rel")
      .addAttribute("k", true, StringAttributeType.INSTANCE)
      .addAttribute("v", false, IntegerAttributeType.INSTANCE)
      .build();

  @Test
  public void testInsert() throws Exception {
    ArgumentCaptor<String> relIdCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Collection<Tuple>> tuplesCaptor = ArgumentCaptor.forClass(Collection.class);

    ValorContext context = StandardContextFactory.create();
    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.createRelation(relation, true);
      ValorConnection spy = spy(conn);
      QueryCommand command = new QueryCommand();
      CmdLineParser parser = new CmdLineParser(command);
      String[] args = {"-e", "insert into rel values ('key', 100)"};
      parser.parseArgument(args);

      command.execute(spy);
      verify(spy, times(1)).insert(relIdCaptor.capture(), tuplesCaptor.capture());
    }

    Collection<Tuple> tuples = tuplesCaptor.getAllValues().get(0);
    assertThat(tuples, hasSize(1));
    Tuple t = tuples.stream().findFirst().get();
    assertEquals("key", t.getAttribute("k"));
    assertEquals(100, t.getAttribute("v"));

  }

  @Test
  public void testSelect() throws Exception {
    ArgumentCaptor<RelationScan> scanCapture = ArgumentCaptor.forClass(RelationScan.class);

    ValorContext context = StandardContextFactory.create();
    try (ValorConnection conn = ValorConnectionFactory.create(context)) {
      conn.createRelation(relation, true);
      conn.createSchema(
          relation.getRelationId(),
          EmbeddedKVDefaultSchemaHandler.buildDefaultSchema(Collections.EMPTY_MAP, relation),
          true);
      ValorConnection spy = spy(conn);
      TupleScanner scanner = mock(TupleScanner.class);
      when(scanner.next()).thenReturn(new TupleImpl(relation));
      QueryCommand command = new QueryCommand();
      CmdLineParser parser = new CmdLineParser(command);
      String[] args = {"-e", "select * from  rel where k = '100' limit 3"};
      parser.parseArgument(args);

      command.execute(spy);
      verify(spy).compile(scanCapture.capture());
      RelationScan scan = scanCapture.getValue();
      assertThat(scan.getLimit(), equalTo(3));
    }
  }

}
