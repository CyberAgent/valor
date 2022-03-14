package jp.co.cyberagent.valor.sdk.metadata;

import static jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde.SCHEMAS;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.relation.ImmutableAttribute;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.ArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaHandler;

public class RelationDeserializer extends JsonDeserializer<Relation> {

  public static final String RELATION_ID = "relationId";
  public static final String ATTRIBUTES = "attributes";
  public static final String ATTR_NAME = "name";
  public static final String ATTR_IS_KEY = "isKey";
  public static final String ATTR_TYPE = "type";
  public static final String ATTR_IS_NULLABLE = "nullable";
  public static final String TYPE_NAME = "name";
  public static final String ARRAY_ELEMENT_TYPE = "element";
  public static final String MAP_KEY_TYPE = "key";
  public static final String MAP_VALUE_TYPE = "value";

  private final String relationId;
  private final ValorContext context;

  public RelationDeserializer(String relationId, ValorContext context) {
    this.relationId = relationId;
    this.context = context;
  }

  @Override
  public Relation deserialize(JsonParser in, DeserializationContext deserializationContext)
      throws IOException {
    try {
      checkToken(JsonToken.START_OBJECT, in.getCurrentToken(), in);
      ImmutableRelation.Builder builder = ImmutableRelation.builder();
      if (relationId != null) {
        builder.relationId(relationId);
      }
      while (in.nextToken() != JsonToken.END_OBJECT) {
        String key = in.getCurrentName();
        if (RELATION_ID.equals(key)) {
          // for backward compatibility
          builder.relationId(in.nextTextValue());
        } else if (ATTRIBUTES.equals(key)) {
          checkToken(JsonToken.START_ARRAY, in.nextToken(), in);
          while (in.nextToken() != JsonToken.END_ARRAY) {
            checkToken(JsonToken.START_OBJECT, in.getCurrentToken(), in);
            ImmutableAttribute.Builder attrBuilder = ImmutableAttribute.builder();
            while (in.nextToken() != JsonToken.END_OBJECT) {
              readAttributeTypeProperty(attrBuilder, in);
            }
            builder.addAttribute(attrBuilder.build());
          }
        } else if (MetadataJsonSerde.HANDLER.equals(key)) {
          checkToken(JsonToken.START_OBJECT, in.nextToken(), in);
          String name = null;
          Map<String, Object> conf = null;
          while (in.nextToken() != JsonToken.END_OBJECT) {
            String hkey = in.getCurrentName();
            if (MetadataJsonSerde.TYPE.equals(hkey)) {
              name = in.nextTextValue();
            } else if (MetadataJsonSerde.CONF.equals(hkey)) {
              in.nextToken();
              conf = readMap(in);
            }
          }
          SchemaHandler handler = context.createSchemaHandler(name, conf);
          builder.schemaHandler(handler);
        } else if (SCHEMAS.equals(key)) {
          checkToken(JsonToken.START_ARRAY, in.nextToken(), in);
          while (in.nextToken() != JsonToken.END_ARRAY) {
            SchemaDescriptorDeserializer schemaDeserializer
                = new SchemaDescriptorDeserializer(null, context);
            SchemaDescriptor schema = schemaDeserializer.deserialize(in, deserializationContext);
            builder.addSchema(schema);
          }
        } else {
          throw new IllegalArgumentException("unexpected relation property " + key);
        }
      }
      return builder.build();
    } catch (IOException e) {
      throw new SerdeException(e);
    }

  }

  private void readAttributeTypeProperty(ImmutableAttribute.Builder builder, JsonParser in)
      throws IOException {
    String key = in.getCurrentName();
    if (ATTR_NAME.equals(key)) {
      builder.name(in.nextTextValue());
    } else if (ATTR_IS_KEY.equals(key)) {
      builder.isKey(in.nextBooleanValue());
    } else if (ATTR_TYPE.equals(key)) {
      builder.type(readAttributeType(in));
    } else if (ATTR_IS_NULLABLE.equals(key)) {
      builder.isNullable(in.nextBooleanValue());
    } else {
      throw new IllegalArgumentException("unexpected attribute property " + key);
    }
  }

  private AttributeType readAttributeType(JsonParser in) throws IOException {
    in.nextToken();
    if (in.getCurrentToken() == JsonToken.VALUE_STRING) {
      return AttributeType.getPrimitiveType(in.getValueAsString());
    } else {
      checkToken(JsonToken.START_OBJECT, in.getCurrentToken(), in);
      String typeName = null;
      AttributeType arrayElementType = null;
      AttributeType mapKeyType = null;
      AttributeType mapValueType = null;
      while (in.nextToken() != JsonToken.END_OBJECT) {
        String key = in.getCurrentName();
        if (TYPE_NAME.equals(key)) {
          typeName = in.nextTextValue();
        } else if (ARRAY_ELEMENT_TYPE.equals(key)) {
          arrayElementType = readAttributeType(in);
        } else if (MAP_KEY_TYPE.equals(key)) {
          mapKeyType = readAttributeType(in);
        } else if (MAP_VALUE_TYPE.equals(key)) {
          mapValueType = readAttributeType(in);
        } else {
          throw new IllegalArgumentException("unexpected attribute type property " + key);
        }
      }
      AttributeType type = AttributeType.getPrimitiveType(typeName);
      if (type != null) {
        return type;
      }
      if (ArrayAttributeType.NAME.equals(typeName)) {
        final ArrayAttributeType arrayType = new ArrayAttributeType();
        arrayType.addGenericElementType(arrayElementType);
        return arrayType;
      } else if (MapAttributeType.NAME.equals(typeName)) {
        final MapAttributeType mapType = new MapAttributeType();
        mapType.addGenericElementType(mapKeyType);
        mapType.addGenericElementType(mapValueType);
        return mapType;
      }
      throw new IllegalArgumentException("unsupported attribute type " + typeName);
    }
  }

  private Map readMap(JsonParser in) throws IOException {
    Map m = new HashMap();
    checkToken(JsonToken.START_OBJECT, in.getCurrentToken(), in);
    while (in.nextToken() != JsonToken.END_OBJECT) {
      String name = in.getCurrentName();
      // TODO support other value type
      String value = in.nextTextValue();
      m.put(name, value);
    }
    return m;
  }


  private void checkToken(JsonToken expected, JsonToken actual, JsonParser in) throws IOException {
    if (actual != expected) {
      String remaining = dumpRemaining(in);
      throw new IllegalArgumentException(expected.name() + " is expected but " + actual.name()
          + ": " + remaining);
    }
  }

  private String dumpRemaining(JsonParser in) throws IOException {
    StringBuilder buf = new StringBuilder();
    do {
      buf.append(in.getCurrentToken()).append(", ");
    } while (in.nextToken() != null);
    return buf.toString();
  }

}
