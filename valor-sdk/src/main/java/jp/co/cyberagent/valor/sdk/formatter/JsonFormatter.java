package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.serde.DisassembleDeserializer.BEGIN_UNNESTING;
import static jp.co.cyberagent.valor.sdk.serde.DisassembleDeserializer.COMMIT_UNNESTING;
import static jp.co.cyberagent.valor.sdk.serde.DisassembleDeserializer.UNNESTING_STATE_KEY;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;

public class JsonFormatter extends AggregationFormatter {

  public static final String FORMATTER_TYPE = "json";
  public static final String ATTRIBUTES_PROPKEY = "attrs";
  public static final String UNNEST_PROPKEY = "unnest";
  public static final String ROOT_PROPKEY = "root";
  public static final String UNNEST_ROOT_PROPKEY = "unnestRoot";
  public static final String RENMAE_PROPKEY = "rename";

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private static final JsonFactory jsonFactory = new JsonFactory();

  private List<String> attributes;
  private Map<String, String> fieldToNestedAttrs;
  private String rootField = null;
  private boolean unnestRoot = false;
  private Map<String, String> rename;

  public JsonFormatter(){
  }

  public JsonFormatter(Map config) {
    setProperties(config);
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(in, offset, length);
         JsonParser parser = jsonFactory.createParser(bais)) {
      parser.setCodec(objectMapper);
      parser.nextToken();
      if (rootField == null) {
        parse(parser, target);
      } else {
        while (parser.nextToken() != JsonToken.END_OBJECT) {
          String field = parser.getCurrentName();
          parser.nextToken();
          if (!rootField.equals(field)) {
            parser.readValueAsTree();
            continue;
          }
          if (unnestRoot) {
            while (parser.nextToken() != JsonToken.END_ARRAY) {
              target.setState(UNNESTING_STATE_KEY, BEGIN_UNNESTING);
              parse(parser, target);
              target.setState(UNNESTING_STATE_KEY, COMMIT_UNNESTING);
            }
          } else {
            parser.nextToken();
            parse(parser, target);
          }
        }
      }
    } catch (IOException e) {
      throw new SerdeException(e);
    }
    return length;
  }

  private void parse(JsonParser parser, TupleDeserializer target) throws IOException {
    Relation relation = target.getRelation();
    if (parser.currentToken() != JsonToken.START_OBJECT) {
      throw new IllegalArgumentException("unexpected token " + parser.currentToken());
    }
    while (parser.nextToken() != JsonToken.END_OBJECT) {
      String field = parser.getCurrentName();
      if (rename.containsKey(field)) {
        field = rename.get(field);
      }
      parser.nextToken();
      if (attributes.contains(field)) {
        Relation.Attribute attr = relation.getAttribute(field);
        Object v = parser.readValueAs(attr.type().getRepresentedClass());
        target.putAttribute(field, v);
      } else if (fieldToNestedAttrs != null && fieldToNestedAttrs.containsKey(field)) {
        String attrName = fieldToNestedAttrs.get(field);
        Relation.Attribute attr = relation.getAttribute(attrName);
        while (parser.nextToken() != JsonToken.END_ARRAY) {
          target.setState(UNNESTING_STATE_KEY, BEGIN_UNNESTING);
          Object v = parser.readValueAs(attr.type().getRepresentedClass());
          target.putAttribute(attrName, v);
          target.setState(UNNESTING_STATE_KEY, COMMIT_UNNESTING);
        }
      } else {
        parser.readValueAsTree();
      }
    }
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    Map<String, Object> attrs = new HashMap<>();
    for (String attr : attributes) {
      if (fieldToNestedAttrs != null && fieldToNestedAttrs.values().contains(attr)) {
        attrs.put(attr, Arrays.asList(tuple.getAttribute(attr)));
      } else {
        attrs.put(attr, tuple.getAttribute(attr));
      }
    }
    try {
      serializer.write(null, objectMapper.writeValueAsBytes(attrs));
    } catch (JsonProcessingException e) {
      throw new ValorException(e);
    }
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws ValorException {
  }

  @Override
  public boolean containsAttribute(String attr) {
    if (attributes.contains(attr)) {
      return true;
    }
    if (fieldToNestedAttrs == null) {
      return false;
    }
    return fieldToNestedAttrs.values().contains(attr);
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> prop = new HashMap<>();
    prop.put(ATTRIBUTES_PROPKEY, attributes);
    if (fieldToNestedAttrs != null) {
      prop.put(UNNEST_PROPKEY, fieldToNestedAttrs);
    }
    if (rootField != null) {
      prop.put(ROOT_PROPKEY, rootField);
      if (unnestRoot) {
        prop.put(UNNEST_ROOT_PROPKEY, unnestRoot);
      }
    }
    if (!rename.isEmpty()) {
      prop.put(RENMAE_PROPKEY, rename);
    }
    return prop;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    this.attributes = (List<String>) props.get(ATTRIBUTES_PROPKEY);
    if (props.containsKey(UNNEST_PROPKEY)) {
      this.fieldToNestedAttrs = (Map<String, String>)props.get(UNNEST_PROPKEY);
    }
    if (props.containsKey(ROOT_PROPKEY)) {
      rootField = (String) props.get(ROOT_PROPKEY);
      unnestRoot = (boolean) props.getOrDefault(UNNEST_ROOT_PROPKEY, false);
    }
    rename = (Map<String, String>) props.getOrDefault(RENMAE_PROPKEY, Collections.emptyMap());
  }

  public static class Factory implements FormatterFactory {
    @Override
    public Formatter create(Map config) {
      return new JsonFormatter(config);
    }

    @Override
    public String getName() {
      return FORMATTER_TYPE;
    }

    @Override
    public Class<? extends Formatter> getProvidedClass() {
      return ConstantFormatter.class;
    }
  }

}
