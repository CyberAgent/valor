package jp.co.cyberagent.valor.cli;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.util.List;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.relation.ImmutableAttribute;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.IntegerAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import org.junit.jupiter.api.Test;
import org.kohsuke.args4j.CmdLineParser;
import org.mockito.ArgumentCaptor;

public class TestInsertCommand {

  static final Relation.Attribute KEY_ATTR = ImmutableAttribute.builder()
      .name("k").isKey(true).isNullable(false).type(StringAttributeType.INSTANCE).build();
  static final Relation.Attribute VAL_ATTR = ImmutableAttribute.builder()
      .name("v").isKey(false).isNullable(true).type(IntegerAttributeType.INSTANCE).build();

  @Test
  public void testExecute() throws Exception {
    ValorConnection client = mock(ValorConnection.class);
    Relation relation = mock(Relation.class);
    ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<List<Tuple>> tupleCaptor = ArgumentCaptor.forClass(List.class);

    when(client.findRelation(anyString())).thenReturn(relation);
    when(relation.getAttribute("k")).thenReturn(KEY_ATTR);
    when(relation.getAttribute("v")).thenReturn(VAL_ATTR);

    InsertCommand testCommand = new InsertCommand();

    CmdLineParser parser = new CmdLineParser(testCommand);
    String[] args = {"-f", "src/test/resources/testRecord.json", "-r", "relation"};
    parser.parseArgument(args);

    testCommand.execute(client);
    verify(client, times(1)).insert(idCaptor.capture(), tupleCaptor.capture());

    String capturedId = idCaptor.getAllValues().get(0);
    Tuple capturedTuple = tupleCaptor.getAllValues().get(0).get(0);
    assertEquals("relation", capturedId);
    assertEquals("valueOfk1", capturedTuple.getAttribute("k"));
    assertEquals(100, capturedTuple.getAttribute("v"));

    capturedTuple = tupleCaptor.getAllValues().get(0).get(1);
    assertEquals("relation", capturedId);
    assertEquals("valueOfk2", capturedTuple.getAttribute("k"));
    assertEquals(200, capturedTuple.getAttribute("v"));

    capturedTuple = tupleCaptor.getAllValues().get(0).get(2);
    assertEquals("relation", capturedId);
    assertEquals("valueOfk3", capturedTuple.getAttribute("k"));
    assertThat(capturedTuple.getAttribute("v"), is(nullValue()));
  }

  @Test
  public void testTypeMismatch() throws Exception {
    ValorConnection client = mock(ValorConnection.class);
    Relation relation = mock(Relation.class);

    when(client.findRelation(anyString())).thenReturn(relation);
    when(relation.getAttribute("k")).thenReturn(KEY_ATTR);
    when(relation.getAttribute("v")).thenReturn(KEY_ATTR);

    InsertCommand testCommand = new InsertCommand();

    CmdLineParser parser = new CmdLineParser(testCommand);
    String[] args = {"-f", "src/test/resources/testRecord.json", "-r", "relation"};
    parser.parseArgument(args);

    assertThrows(Exception.class, () -> testCommand.execute(client));
  }

  @Test
  public void testExecuteOneOfEach() throws Exception {
    ValorConnection client = mock(ValorConnection.class);
    Relation relation = mock(Relation.class);
    ArgumentCaptor<String> idCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<List<Tuple>> tupleCaptor = ArgumentCaptor.forClass(List.class);

    when(client.findRelation(anyString())).thenReturn(relation);
    when(relation.getAttribute("k")).thenReturn(KEY_ATTR);
    when(relation.getAttribute("v")).thenReturn(VAL_ATTR);

    InsertCommand testCommand = new InsertCommand();

    CmdLineParser parser = new CmdLineParser(testCommand);
    String[] args = {"-f", "src/test/resources/testRecord.json", "-r", "relation", "-n", "1"};
    parser.parseArgument(args);

    testCommand.execute(client);
    verify(client, times(3)).insert(idCaptor.capture(), tupleCaptor.capture());

    String capturedId = idCaptor.getAllValues().get(0);
    Tuple capturedTuple = tupleCaptor.getAllValues().get(0).get(0);
    assertEquals("relation", capturedId);
    assertEquals("valueOfk1", capturedTuple.getAttribute("k"));
    assertEquals(100, capturedTuple.getAttribute("v"));

    capturedId = idCaptor.getAllValues().get(1);
    capturedTuple = tupleCaptor.getAllValues().get(1).get(0);
    assertEquals("relation", capturedId);
    assertEquals("valueOfk2", capturedTuple.getAttribute("k"));
    assertEquals(200, capturedTuple.getAttribute("v"));
  }
}
