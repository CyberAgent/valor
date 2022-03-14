package jp.co.cyberagent.valor.sdk.metadata;

import static jp.co.cyberagent.valor.sdk.metadata.RelationDeserializer.RELATION_ID;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Formatter;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Segment;

public class SchemaDescriptorDeserializer extends JsonDeserializer<SchemaDescriptor> {

  public static final String SCHEMA_ID = "schemaId";
  public static final String MODE = "mode";
  public static final String PRIMARY = "isPrimary";
  public static final String STORAGE = "storage";
  public static final String FIELDS = "fields";
  public static final String FIELD_NAME = "name";
  public static final String FORMAT = "format";
  public static final String FORMATTER_TYPE = "type";
  @Deprecated
  public static final String DECORATEE = "decoratee";
  public static final String VALUE = "value";
  public static final String CLASS = "class";
  public static final String CONF = "conf";
  public static final String PROPS = "props";

  private final String schemaId;

  private final ValorContext context;

  public SchemaDescriptorDeserializer(String schemaId, ValorContext context) {
    this.schemaId = schemaId;
    this.context = context;
  }

  @Override
  public SchemaDescriptor deserialize(JsonParser in, DeserializationContext deserializationContext)
      throws IOException, JsonProcessingException {
    try {
      checkToken(JsonToken.START_OBJECT, in.getCurrentToken(), in);
      ImmutableSchemaDescriptor.Builder builder = ImmutableSchemaDescriptor.builder();
      if (schemaId != null) {
        builder.schemaId(schemaId);
      }
      while (in.nextToken() != JsonToken.END_OBJECT) {
        String key = in.getCurrentName();
        if (SCHEMA_ID.equals(key)) {
          builder.schemaId(in.nextTextValue());
        } else if (RELATION_ID.equals(key)) {
          in.nextTextValue(); // skip deprecated field for backward compatibility
        } else if (PRIMARY.equals(key)) {
          builder.isPrimary(in.nextBooleanValue());
        } else if (MODE.equals(key)) {
          builder.mode(Schema.Mode.valueOf(in.nextTextValue()));
        } else if (STORAGE.equals(key)) {
          checkToken(JsonToken.START_OBJECT, in.nextToken(), in);
          while (in.nextToken() != JsonToken.END_OBJECT) {
            String storageKey = in.getCurrentName();
            if (CLASS.equals(storageKey)) {
              builder.storageClassName(in.nextTextValue());
            } else if (CONF.equals(storageKey)) {
              checkToken(JsonToken.START_OBJECT, in.nextToken(), in);
              builder.storageConf(readConf(in));
            } else {
              throw new IllegalArgumentException("unexpected storage descriptor property " + key);
            }
          }
        } else if (FIELDS.equals(key)) {
          checkToken(JsonToken.START_ARRAY, in.nextToken(), in);
          while (in.nextToken() != JsonToken.END_ARRAY) {
            builder.addFields(readFieldLayout(in));
          }
        } else if (CONF.equals(key)) {
          checkToken(JsonToken.START_OBJECT, in.nextToken(), in);
          builder.conf(readConf(in));
        } else {
          throw new IllegalArgumentException("unexpected schema property " + key);
        }
      }
      return builder.build();
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  public FieldLayout readFieldLayout(JsonParser in) throws IOException {
    String name = null;
    List<Segment> recordFormatters = new ArrayList<>();
    checkToken(JsonToken.START_OBJECT, in.getCurrentToken(), in);
    while (in.nextToken() != JsonToken.END_OBJECT) {
      String key = in.getCurrentName();
      if (FIELD_NAME.equals(key)) {
        name = in.nextTextValue();
      } else if (FORMAT.equals(key)) {
        checkToken(JsonToken.START_ARRAY, in.nextToken(), in);
        while (in.nextToken() != JsonToken.END_ARRAY) {
          recordFormatters.add(readAttributeFormatter(in));
        }
      } else {
        throw new IllegalArgumentException("unexpected field property " + key);
      }
    }
    if (name == null) {
      throw new IllegalArgumentException("field name is not set");
    }
    return FieldLayout.of(name, recordFormatters);
  }

  public Segment readAttributeFormatter(JsonParser in) throws IOException {
    checkToken(JsonToken.START_OBJECT, in.getCurrentToken(), in);
    String type = null;
    Map<String, Object> props = null;
    Segment value = null;
    while (in.nextToken() != JsonToken.END_OBJECT) {
      String key = in.getCurrentName();
      if (FORMATTER_TYPE.equals(key)) {
        type = in.nextTextValue();
      } else if (PROPS.equals(key)) {
        in.nextToken();
        //om.readValue(in, Map.class);
        props = in.readValueAs(Map.class);
      } else if (DECORATEE.equals(key) || VALUE.equals(key)) {
        in.nextToken();
        value = readAttributeFormatter(in);
      } else {
        throw new IllegalArgumentException(
            "unexpected formatter property " + key + "\n" + dumpRemaining(in));
      }
    }
    if (value == null) {
      Segment e = context.createFormatter(type, props);
      return e;
    } else {
      Holder h = context.createHolder(type, props);
      h.setFormatter((Formatter) value);
      return h;
    }
  }

  private ValorConf readConf(JsonParser in) throws IOException {
    ValorConf conf = new ValorConfImpl();
    checkToken(JsonToken.START_OBJECT, in.getCurrentToken(), in);
    while (in.nextToken() != JsonToken.END_OBJECT) {
      String name = in.getCurrentName();
      String value = in.nextTextValue();
      conf.set(name, value);
    }
    return conf;
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
