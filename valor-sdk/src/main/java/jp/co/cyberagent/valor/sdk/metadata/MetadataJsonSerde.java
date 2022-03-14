package jp.co.cyberagent.valor.sdk.metadata;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.metadata.MetadataSerde;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.ArrayAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;
import jp.co.cyberagent.valor.spi.relation.type.MapAttributeType;
import jp.co.cyberagent.valor.spi.schema.DefaultSchemaHandler;
import jp.co.cyberagent.valor.spi.schema.FieldLayout;
import jp.co.cyberagent.valor.spi.schema.Holder;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.SchemaHandler;
import jp.co.cyberagent.valor.spi.schema.Segment;

/**
 *
 */
public class MetadataJsonSerde implements MetadataSerde {

  public static final String TYPE = "type";
  public static final String RELATION_ID = "relationId";
  public static final String ATTRIBUTES = "attributes";
  public static final String ATTR_NAME = "name";
  public static final String ATTR_IS_KEY = "isKey";
  public static final String ATTR_TYPE = "type";
  public static final String ATTR_IS_NULLABLE = "nullable";
  public static final String HANDLER = "handler";
  public static final String TYPE_NAME = "name";
  public static final String ARRAY_ELEMENT_TYPE = "element";
  public static final String MAP_KEY_TYPE = "key";
  public static final String MAP_VALUE_TYPE = "value";
  public static final String SCHEMAS = "schemas";
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

  public ObjectMapper om = new ObjectMapper();
  public static final String NAME = "json";
  private static final JsonFactory jsonFactory = new JsonFactory();
  private final ValorContext context;

  public MetadataJsonSerde(ValorContext context) {
    this.context = context;
  }

  @Override
  public byte[] serialize(Relation relation) throws SerdeException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); JsonGenerator generator =
        jsonFactory.createGenerator(baos)) {
      serializeInto(generator, relation);
      generator.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  @Override
  public byte[] serialize(SchemaDescriptor schema) throws SerdeException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); JsonGenerator generator =
        jsonFactory.createGenerator(baos)) {
      writeSchema(generator, schema);
      generator.flush();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  private void serializeInto(JsonGenerator out, Relation relation) throws IOException {
    out.writeStartObject();
    out.writeStringField(RELATION_ID, relation.getRelationId());
    out.writeFieldName(ATTRIBUTES);
    out.writeStartArray();
    for (String attribute : relation.getAttributeNames()) {
      final Relation.Attribute a = relation.getAttribute(attribute);
      out.writeStartObject();
      out.writeStringField(ATTR_NAME, attribute);
      out.writeBooleanField(ATTR_IS_KEY, a.isKey());
      writeAttributeType(out, ATTR_TYPE, a.type());
      out.writeBooleanField(ATTR_IS_NULLABLE, a.isNullable());
      out.writeEndObject();
    }
    out.writeEndArray();
    SchemaHandler handler = relation.getSchemaHandler();
    if (handler != null && !DefaultSchemaHandler.NAME.equals(handler.getName())) {
      out.writeFieldName(HANDLER);
      out.writeStartObject();
      out.writeStringField(TYPE, handler.getName());
      out.writeFieldName(CONF);
      om.writeValue(out, handler.getConf());
      out.writeEndObject();
    }
    out.writeEndObject();
  }

  private void writeAttributeType(JsonGenerator out, String jsonAttrName, AttributeType type)
      throws IOException {
    if (AttributeType.getPrimitiveType(type.getName()) != null) {
      out.writeStringField(jsonAttrName, type.getName());
    } else if (type instanceof ArrayAttributeType) {
      final ArrayAttributeType arrayType = (ArrayAttributeType) type;
      out.writeFieldName(jsonAttrName);
      out.writeStartObject();
      out.writeStringField(TYPE_NAME, ArrayAttributeType.NAME);
      writeAttributeType(out, ARRAY_ELEMENT_TYPE, arrayType.getElementType());
      out.writeEndObject();
    } else if (type instanceof MapAttributeType) {
      final MapAttributeType mapType = (MapAttributeType) type;
      out.writeFieldName(jsonAttrName);
      out.writeStartObject();
      out.writeStringField(TYPE_NAME, MapAttributeType.NAME);
      writeAttributeType(out, MAP_KEY_TYPE, mapType.getKeyType());
      writeAttributeType(out, MAP_VALUE_TYPE, mapType.getValueType());
      out.writeEndObject();
    } else {
      throw new UnsupportedOperationException(
          type.toExpression() + " is not serializable currently");
    }
  }

  @Override
  public Relation readRelation(String relId, Reader reader) throws SerdeException {
    try (JsonParser parser = jsonFactory.createParser(reader)) {
      return readRelation(relId, parser);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  public Relation readRelation(String relationId, JsonParser in) throws SerdeException {
    om = new ObjectMapper().registerModule(
        new SimpleModule()
            .addDeserializer(Relation.class, new RelationDeserializer(relationId, context))
    );
    in.setCodec(om);
    try {
      in.nextToken();
      return in.readValueAs(Relation.class);
    } catch (IOException e) {
      throw new SerdeException(e);
    }

  }

  private void writeSchema(JsonGenerator out, SchemaDescriptor schema) throws IOException {
    out.writeStartObject();
    out.writeBooleanField(PRIMARY, schema.isPrimary());
    out.writeStringField(MODE, schema.getMode().name());
    out.writeFieldName(STORAGE);
    out.writeStartObject();
    out.writeStringField(CLASS, schema.getStorageClassName());
    if (schema.getStorageConf() != null) {
      out.writeFieldName(CONF);
      writeConf(out, schema.getStorageConf());
    }
    out.writeEndObject();
    out.writeFieldName(FIELDS);
    out.writeStartArray();
    List<FieldLayout> fields = schema.getFields();
    for (FieldLayout field : fields) {
      writeFieldLayout(out, field);
    }
    out.writeEndArray();
    out.writeFieldName(CONF);
    writeConf(out, schema.getConf());
    out.writeEndObject();
  }

  @Override
  public SchemaDescriptor readSchema(String schemaId, Reader reader)
      throws SerdeException {
    om = new ObjectMapper().registerModule(new SimpleModule()
        .addDeserializer(
            SchemaDescriptor.class, new SchemaDescriptorDeserializer(schemaId, context))
    );
    try (JsonParser in = jsonFactory.createParser(reader)) {
      in.setCodec(om);
      in.nextToken();
      return in.readValueAs(SchemaDescriptor.class);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  public void writeFieldLayout(JsonGenerator out, FieldLayout formatter) throws IOException {
    out.writeStartObject();
    out.writeStringField(FIELD_NAME, formatter.getFieldName());
    out.writeFieldName(FORMAT);
    out.writeStartArray();
    for (Segment attrFormatter : formatter.getFormatters()) {
      writeAttributeFormatter(out, attrFormatter);
    }
    out.writeEndArray();
    out.writeEndObject();
  }

  public void writeAttributeFormatter(JsonGenerator out, Segment formatter) throws IOException {
    out.writeStartObject();
    String type = formatter.getName();
    if (type == null) {
      type = formatter.getClass().getCanonicalName();
    }
    if (formatter instanceof Holder) {
      out.writeStringField(FORMATTER_TYPE, type);
      out.writeFieldName(PROPS);
      om.writeValue(out, formatter.getProperties());
      out.writeFieldName(VALUE);
      writeAttributeFormatter(out, formatter.getFormatter());
    } else {
      out.writeStringField(FORMATTER_TYPE, type);
      out.writeFieldName(PROPS);
      om.writeValue(out, formatter.getProperties());
    }
    out.writeEndObject();
  }

  private void writeConf(JsonGenerator out, ValorConf conf) throws IOException {
    out.writeStartObject();
    for (Map.Entry<String, String> props : conf) {
      out.writeStringField(props.getKey(), props.getValue());
    }
    out.writeEndObject();
  }

}
