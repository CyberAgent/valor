package jp.co.cyberagent.valor.cli.ast;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import jp.co.cyberagent.valor.spi.plan.model.Expression;

public abstract class AstNodeDeserializer<T extends AstNode> extends JsonDeserializer<T> {

  public static class ExpressionDeserializer extends AstNodeDeserializer<ExpressionNode> {

    @Override
    public ExpressionNode deserialize(JsonParser jsonParser,
                                      DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {
      return deserializeExpression(jsonParser, deserializationContext);
    }
  }

  public static class QueryDeserializer extends AstNodeDeserializer<ScanNode> {

    @Override
    public ScanNode deserialize(JsonParser jsonParser,
                                DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException {
      return deserializeQuery(jsonParser, deserializationContext);
    }
  }

  protected ScanNode deserializeQuery(JsonParser parser, DeserializationContext context)
      throws IOException {
    ProjectionClauseNode select = null;
    FromClauseNode from = null;
    WhereClauseNode where = null;
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      String fieldName = parser.getCurrentName();
      if ("select".equals(fieldName)) {
        parser.nextToken();
        checkToken(parser, JsonToken.START_ARRAY, context);
        List<ExpressionNode> items = new ArrayList<>();
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          String attr = parser.getText();
          items.add(new AttributeNode(attr));
        }
        select = new ProjectionClauseNode(items);
      } else if ("from".equals(fieldName)) {
        from = new RelationSourceNode(parser.nextTextValue());
      } else if ("where".equals(fieldName)) {
        parser.nextToken();
        checkToken(parser, JsonToken.START_OBJECT, context);
        ExpressionNode condition = deserializeExpression(parser, context);
        where = new WhereClauseNode(condition);
      }
    }
    return new ScanNode(select, from, where);
  }

  protected ExpressionNode deserializeExpression(JsonParser parser, DeserializationContext context)
      throws IOException {
    String type = null;
    AttributeNode attr = null;
    ConstantNode value = null;
    List<ExpressionNode> predicates = new ArrayList<>();
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      String fieldName = parser.getCurrentName();
      if ("type".equals(fieldName)) {
        type = parser.nextTextValue();
      } else if ("key".equals(fieldName) || "attr".equals(fieldName)) {
        attr = new AttributeNode(parser.nextTextValue());
      } else if ("value".equals(fieldName)) {
        JsonToken node = parser.nextValue();
        if (node.isBoolean()) {
          value = new ConstantNode(Boolean.valueOf(JsonToken.VALUE_TRUE == node));
        } else if (node.isNumeric()) {
          value = new ConstantNode(parser.getNumberValue());
        } else if (node.isScalarValue()) {
          value = new ConstantNode(parser.getText());
        } else {
          throw JsonMappingException.from(parser,"expected primitive type, but " + node);
        }
      } else if ("predicate".equals(fieldName) || "query".equals(fieldName)) {
        parser.nextToken();
        checkToken(parser, JsonToken.START_OBJECT, context);
        predicates.add(deserializeExpression(parser, context));
      } else if ("predicates".equals(fieldName) || "queries".equals(fieldName)) {
        parser.nextToken();
        checkToken(parser, JsonToken.START_ARRAY, context);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          predicates.add(deserializeExpression(parser, context));
        }
      } else {
        throw JsonMappingException.from(parser,"unexpected token " + parser.getCurrentToken());
      }
    }

    if (SemanticAnalyzer.BINARY_COMPARE_OPERATOR.contains(type)) {
      return new FunctionNode(type, attr, value);
    } else if (SemanticAnalyzer.UNARY_OPERATOR.contains(type)) {
      return new FunctionNode(type, attr);
    } else if (SemanticAnalyzer.LOGICAL_OPERATOR.contains(type)) {
      return new FunctionNode(type, predicates);
    } else {
      throw JsonMappingException.from(parser, "unexpected type " + type);
    }
  }

  private void checkToken(JsonParser parser, JsonToken expected, DeserializationContext context)
      throws IOException {
    if (parser.getCurrentToken() != expected) {
      throw context.wrongTokenException(parser, Expression.class, expected,
          "failed to parse a predicate");
    }
  }

}
