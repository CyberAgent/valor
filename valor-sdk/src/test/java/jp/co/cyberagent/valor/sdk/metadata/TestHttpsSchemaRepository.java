package jp.co.cyberagent.valor.sdk.metadata;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.formatter.AttributeValueFormatter;
import jp.co.cyberagent.valor.sdk.formatter.ConstantFormatter;
import jp.co.cyberagent.valor.sdk.storage.kv.EmbeddedEKVStorage;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.schema.ImmutableSchemaDescriptor;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

// Http Schema Repository is read only repository
public class TestHttpsSchemaRepository {

  private static final String REL_ID = "rel1";
  private static final String SCHEMA1_ID = "schema1";
  private static final String SCHEMA2_ID = "schema2";

  private static String relationDef;
  private static String schemaDef1;
  private static String schemaDef2;

  private static Relation rel;
  private static SchemaDescriptor schema1;
  private static SchemaDescriptor schema2;
  private static MetadataJsonSerde jsonSerde;

  @BeforeAll
  public static void init() throws ValorException {
    ValorConf conf = new ValorConfImpl();
    ValorContext context = StandardContextFactory.create(conf);
    jsonSerde = new MetadataJsonSerde(context);
    rel = ImmutableRelation.builder().relationId(REL_ID)
        .addAttribute("k", true , StringAttributeType.INSTANCE)
        .addAttribute("v", false, StringAttributeType.INSTANCE)
        .build();
    relationDef = new String(jsonSerde.serialize(rel));
    schema1 = ImmutableSchemaDescriptor.builder()
        .schemaId(SCHEMA1_ID)
        .conf(conf)
        .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
        .storageConf(conf)
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(AttributeValueFormatter.create("k")))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(ConstantFormatter.create("1")))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(AttributeValueFormatter.create("v")))
        .build();
    schemaDef1 = new String(jsonSerde.serialize(schema1));
    schema2 = ImmutableSchemaDescriptor.builder()
        .schemaId(SCHEMA2_ID)
        .conf(conf)
        .storageClassName(EmbeddedEKVStorage.class.getCanonicalName())
        .storageConf(conf)
        .addField(EmbeddedEKVStorage.KEY, Arrays.asList(AttributeValueFormatter.create("k")))
        .addField(EmbeddedEKVStorage.COL, Arrays.asList(ConstantFormatter.create("2")))
        .addField(EmbeddedEKVStorage.VAL, Arrays.asList(AttributeValueFormatter.create("v")))
        .build();
    schemaDef2 = new String(jsonSerde.serialize(schema2));
  }

  static abstract class MockDispatcher extends Dispatcher {

    @NotNull
    @Override
    public MockResponse dispatch(@NotNull RecordedRequest request) {
      MockResponse response = preprocess(request);
      if (response != null) {
        return response;
      }
      String path = request.getPath();
      try {
        return buildMockResponse(path);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }
    abstract MockResponse preprocess(RecordedRequest request);
  }

  @NotNull
  private static MockResponse buildMockResponse(String path) throws JsonProcessingException {
    int hasParam = path.indexOf("?");
    if (hasParam > 0) {
      path = path.substring(0, path.indexOf("?"));
    }
    if (path.equals(String.format("/v1/relations.json", REL_ID))) {
      return new MockResponse().setResponseCode(200).setBody(String.format("[\"%s\"]", REL_ID));
    } else if (path.equals(String.format("/v1/relations/%s/relation.json", REL_ID))) {
      return new MockResponse().setResponseCode(200).setBody(relationDef);
    } else if (path.equals(String.format("/v1/relations/%s/schemas.json", REL_ID))) {
      return new MockResponse().setResponseCode(200)
          .setBody(String.format("[\"%s\",\"%s\"]", SCHEMA1_ID, SCHEMA2_ID));
    } else if (path.equals(
        String.format("/v1/relations/%s/schemas/%s.json", REL_ID, SCHEMA1_ID))) {
      return new MockResponse().setResponseCode(200).setBody(schemaDef1);
    } else if (path.equals(
        String.format("/v1/relations/%s/schemas/%s.json", REL_ID, SCHEMA2_ID))) {
      return new MockResponse().setResponseCode(200).setBody(schemaDef2);
    }
    return new MockResponse().setResponseCode(404);
  }


  @Test
  public void testHttp() throws Exception {
    final Dispatcher dispatcher = new MockDispatcher() {
      @Override
      MockResponse preprocess(RecordedRequest request) {
        return null;
      }
    };
    ValorConf conf = new ValorConfImpl();
    testServer(conf, dispatcher);
  }
  

  @Test
  public void testEmptySchema() throws Exception {
    final Dispatcher dispatcher = new MockDispatcher() {
      @Override
      MockResponse preprocess(RecordedRequest request) {
        String path = request.getPath();
        if (path.equals(String.format("/v1/relations.json", REL_ID))) {
          return new MockResponse().setResponseCode(200).setBody(String.format("[\"%s\"]", REL_ID));
        } else if (path.equals(String.format("/v1/relations/%s/relation.json", REL_ID))) {
          return new MockResponse().setResponseCode(200).setBody(relationDef);
        }
        return new MockResponse().setResponseCode(404);
      }
    };
    ValorConf conf = new ValorConfImpl();
    testServer(conf, dispatcher, REL_ID);
  }

  @Test
  public void testAuthentication() throws Exception {
    final Dispatcher dispatcher = new MockDispatcher() {
      @Override
      MockResponse preprocess(RecordedRequest request) {
        String header = request.getHeader("Authorization");
        if (!"token TOKEN".equals(header)) {
          return new MockResponse().setResponseCode(401);
        }
        return null;
      }
    };
    ValorConf conf = new ValorConfImpl();
    try {
      testServer(conf, dispatcher);
      fail("authentication passed unexceptionally");
    } catch (ValorException e) {
      assertTrue(e.getMessage().contains("401"));
    }
    try (MockedStatic<HttpsSchemaRepository> mocked
             = Mockito.mockStatic(HttpsSchemaRepository.class)) {
      mocked.when(() -> HttpsSchemaRepository.getEnv("TEST_TOKEN")).thenReturn("TOKEN");
      conf.set(HttpsSchemaRepository.HTTP_REPOS_HEADER_PREFIX + "Authorization", "token ${TEST_TOKEN}");
      testServer(conf, dispatcher);
    }
  }


  @Test
  public void testParameter() throws Exception {
    final Dispatcher dispatcher = new MockDispatcher() {
      @Override
      MockResponse preprocess(RecordedRequest request) {
        String ref = request.getRequestUrl().queryParameter("ref");
        if (!"develop".equals(ref)) {
          return new MockResponse().setResponseCode(404);
        }
        return null;
      }
    };
    ValorConf conf = new ValorConfImpl();
    try {
      testServer(conf, dispatcher);
      fail("authentication passed unexceptionally");
    } catch (ValorException e) {
      assertTrue(e.getCause() instanceof FileNotFoundException);
    }
    conf.set(HttpsSchemaRepository.HTTP_REPOS_PARAM_PREFIX + "ref", "develop");
    testServer(conf, dispatcher);
  }


  private void testServer(ValorConf conf, Dispatcher dispatcher) throws Exception {
    testServer(conf, dispatcher, REL_ID, schema1, schema2);
  }

  private void testServer(
      ValorConf conf, Dispatcher dispatcher, String relId, SchemaDescriptor... schemaDescriptors)
      throws Exception {
    try (MockWebServer server = new MockWebServer()) {
      server.setDispatcher(dispatcher);
      server.start();

      String host = server.getHostName();
      int port = server.getPort();
      conf.set(ValorContext.SCHEMA_REPOSITORY_CLASS_KEY, "http");
      conf.set(
          HttpsSchemaRepository.HTTP_REPOS_BASEPATH, String.format("http://%s:%s/v1", host, port));
      ValorContext context = StandardContextFactory.create(conf);
      SchemaRepository repo = context.createRepository(conf);

      // test list relations
      Collection<String> rels = repo.listRelationIds();
      assertThat(rels, hasSize(1));
      assertThat(rels, hasItem(equalTo(relId)));

      // test find relation
      Relation r = repo.findRelation(relId);
      assertEquals(rel, r);

      // test list schemas
      Collection<Schema> schemas = repo.listSchemas(relId);
      assertThat(schemas, hasSize(schemaDescriptors.length));

      List<String> sds = schemas.stream()
          .map(SchemaDescriptor::from)
          .map(s -> new String(jsonSerde.serialize(s)))
          .collect(Collectors.toList());
      for (SchemaDescriptor schemaDescriptor : schemaDescriptors) {
        String schemaDef = new String(jsonSerde.serialize(schemaDescriptor));
        assertThat(sds, hasItem(equalTo(schemaDef)));
        // test get schema
        Schema schema = repo.findSchema(REL_ID, schemaDescriptor.getSchemaId());
        String schemaJson = new String(jsonSerde.serialize(SchemaDescriptor.from(schema)));
        assertThat(schemaDef, equalTo(schemaJson));
      }




    }
  }
}
