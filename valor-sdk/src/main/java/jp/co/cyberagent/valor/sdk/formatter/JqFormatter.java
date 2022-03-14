package jp.co.cyberagent.valor.sdk.formatter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.plan.model.PrimitivePredicate;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.FormatterFactory;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.TrueSegment;
import jp.co.cyberagent.valor.spi.serde.QuerySerializer;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import net.thisptr.jackson.jq.BuiltinFunctionLoader;
import net.thisptr.jackson.jq.Expression;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.Version;
import net.thisptr.jackson.jq.Versions;
import net.thisptr.jackson.jq.internal.javacc.ExpressionParser;
import net.thisptr.jackson.jq.internal.tree.FieldConstruction;
import net.thisptr.jackson.jq.internal.tree.IdentifierKeyFieldConstruction;
import net.thisptr.jackson.jq.internal.tree.ObjectConstruction;
import net.thisptr.jackson.jq.module.loaders.BuiltinModuleLoader;

public class JqFormatter extends Formatter {

  public static final String FORMATTER_TYPE = "jq";

  public static final String JQ_PROPKEY = "jq";

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static Scope rootScope;

  static {
    rootScope = Scope.newEmptyScope();
    BuiltinFunctionLoader.getInstance().loadFunctions(Versions.JQ_1_6, rootScope);
    rootScope.setModuleLoader(BuiltinModuleLoader.getInstance());
  }

  private transient JsonQuery jq;
  private transient Collection<String> attrs;
  private String queryString;

  static JqFormatter create(String jq) {
    JqFormatter formatter = new JqFormatter();
    formatter.parse(jq);
    return formatter;
  }

  public JqFormatter(){
  }

  public JqFormatter(Map config) {
    setProperties(config);
  }

  @Override
  public Order getOrder() {
    return Order.RANDOM;
  }

  @Override
  public int cutAndSet(byte[] in, int offset, int length, TupleDeserializer target)
      throws SerdeException {
    JsonNode jsonNode;
    List<JsonNode> result = new ArrayList<>();
    try {
      jsonNode = objectMapper.readTree(in, offset, length);
      jq.apply(Scope.newChildScope(rootScope), jsonNode, result::add);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
    if (result.size() != 1) {
      String resultStr = result.stream().map(jn -> jn.toString()).collect(Collectors.joining());
      throw new SerdeException("only one json object should be extracted but " + resultStr);
    }
    JsonNode node = result.get(0);
    Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      AttributeType type = target.getRelation().getAttributeType(field.getKey());
      Object v = toPojo(field.getValue(), type.getRepresentedClass());
      if (v != null) {
        target.putAttribute(field.getKey(), v);
      }
    }
    return length;
  }

  private Object toPojo(JsonNode node, Class cls) {
    try {
      return objectMapper.treeToValue(node, cls);
    } catch (JsonProcessingException e) {
      throw new SerdeException(e);
    }
  }

  @Override
  public void accept(TupleSerializer serializer, Tuple tuple) throws ValorException {
    throw new UnsupportedOperationException("jq formatter does not support serialization");
  }

  @Override
  public void accept(QuerySerializer serializer, Collection<PrimitivePredicate> conjunction)
      throws ValorException {
    serializer.write(null, TrueSegment.INSTANCE);
  }

  @Override
  public boolean containsAttribute(String attr) {
    return attrs.contains(attr);
  }

  @Override
  public String getName() {
    return FORMATTER_TYPE;
  }

  @Override
  public Map<String, Object> getProperties() {
    Map<String, Object> prop = new HashMap<>();
    prop.put(JQ_PROPKEY, queryString);
    return prop;
  }

  @Override
  public void setProperties(Map<String, Object> props) {
    String queryString = (String) props.get(JQ_PROPKEY);
    parse(queryString);
  }

  private void parse(String queryString) {
    this.queryString = queryString;
    try {
      Expression exp = ExpressionParser.compile(queryString, Version.LATEST);
      if (!(exp instanceof ObjectConstruction)) {
        throw new IllegalArgumentException("object expression is expected but " + queryString);
      }
      ObjectConstruction oc = (ObjectConstruction) exp;
      attrs = oc.fields.stream().map(extractKey).collect(Collectors.toList());
      this.jq = JsonQuery.compile(queryString, Version.LATEST);
    } catch (Exception e) {
      throw new IllegalArgumentException(e);
    }
  }

  static Function<FieldConstruction, String> extractKey = (f) -> {
    if (f instanceof IdentifierKeyFieldConstruction) {
      return ((IdentifierKeyFieldConstruction) f).key;
    }
    throw new IllegalArgumentException(f.getClass() + " is not supported key type (exp " + f + ")");
  };

  public static class Factory implements FormatterFactory {
    @Override
    public Formatter create(Map config) {
      return new JqFormatter(config);
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
