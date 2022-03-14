package jp.co.cyberagent.valor.ql.parse;

import java.util.ArrayList;
import java.util.List;
import jp.co.cyberagent.valor.ql.grammer.gen.ValorLexer;
import jp.co.cyberagent.valor.ql.grammer.gen.ValorParser;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.plan.model.LogicalPlanNode;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Parser {

  static Logger LOG = LoggerFactory.getLogger(Parser.class);

  private final ValorConnection conn;
  private final List<ParseError> parseErrors = new ArrayList<>();

  public Parser(ValorConnection conn) {
    this.conn = conn;
  }

  public LogicalPlanNode parseStatement(String query) throws RecognitionException {
    ANTLRInputStream input = new AntlrNoCaseStream(query);
    ValorLexer lexer = new ValorLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ValorParser parser = new ValorParser(tokens);
    parser.addErrorListener(new ErrorListener());
    ParseTree tree = parser.execStatement();

    ParseTreeWalker walker = new ParseTreeWalker();
    BuildStatementListener builder = new BuildStatementListener(conn);
    walker.walk(builder, tree);
    return builder.buildStatement();
  }

  private class ErrorListener extends BaseErrorListener {

    @Override
    public void syntaxError(
        Recognizer<?, ?> recognizer,
        Object offendingSymbol,
        int line,
        int charPositionInLine,
        String msg,
        RecognitionException e
    ) {
      parseErrors.add(new ParseError(line, charPositionInLine, msg));
    }

  }

  static class AntlrNoCaseStream extends ANTLRInputStream {

    public AntlrNoCaseStream(String input) {
      super(input);
    }

    //CHECKSTYLE:OFF
    public int LA(int i) {
      if ( i == 0 ) return 0; // undefined
      if ( i < 0 ) i++; // e.g., translate LA(-1) to use offset 0
      if ( (p + i - 1) >= n ) return CharStream.EOF;
      return Character.toUpperCase(data[p+i-1]);
    }
    //CHECKSTYLE:ON
  }


  private static class ParseError {

    final int line;
    final int pos;
    final String message;

    public ParseError(int line, int charPositionInLine, String msg) {
      this.line = line;
      this.pos = charPositionInLine;
      this.message = msg;
    }
  }
}
