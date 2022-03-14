package jp.co.cyberagent.valor.sdk.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpsSchemaRepository extends SchemaRepositoryBase {

  static Logger LOG = LoggerFactory.getLogger(HttpsSchemaRepository.class);

  public static final String HTTP_REPOS_BASEPATH = "valor.http.repo.basepath";
  public static final String HTTP_REPOS_HEADER_PREFIX = "valor.http.repo.headers.";
  public static final String HTTP_REPOS_PARAM_PREFIX = "valor.http.repo.params.";

  private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{([\\w]+)\\}");

  public static final String NAME = "http";

  private static final TypeReference<List<String>> STRING_LIST_TYPE
      = new TypeReference<List<String>>(){};

  private String basePath;

  private Map<String, String> headers = new HashMap<>();
  
  private String params;

  private ObjectMapper om = new ObjectMapper();

  protected HttpsSchemaRepository(ValorConf conf) throws ValorException {
    super(conf);
    String path = conf.get(HTTP_REPOS_BASEPATH);
    if (path == null) {
      throw new IllegalArgumentException(HTTP_REPOS_BASEPATH + " is not set");
    }
    this.basePath = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
    StringBuilder paramString = new StringBuilder();
    for (Map.Entry<String, String> p : conf) {
      if (p.getKey().startsWith(HTTP_REPOS_HEADER_PREFIX)) {
        String v = p.getValue();
        v = evaluateVariable(v);
        headers.put(p.getKey().substring(HTTP_REPOS_HEADER_PREFIX.length()), v);
      }
      if (p.getKey().startsWith(HTTP_REPOS_PARAM_PREFIX)) {
        String v = p.getValue();
        v = evaluateVariable(v);
        paramString
            .append(encode(p.getKey().substring(HTTP_REPOS_PARAM_PREFIX.length())))
            .append("=")
            .append(encode(v));
      }
    }
    if (paramString.length() > 0) {
      this.params = paramString.toString();
    }

  }

  private String encode(String v) {
    try {
      return URLEncoder.encode(v, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException(e);
    }
  }

  private String evaluateVariable(String v) {
    Matcher matcher = VAR_PATTERN.matcher(v);
    while (matcher.find()) {
      String varName = matcher.group(1);
      String varValue = getEnv(varName);
      v = matcher.replaceFirst(varValue);
    }
    return v;
  }

  // for test
  public static String getEnv(String name) {
    return System.getenv(name);
  }

  abstract class Request<V> {

    public V request(String path) throws ValorException {
      URL url = null;
      try {
        url = params == null ? new URL(path) : new URL(path + "?" + params);
      } catch (MalformedURLException e) {
        throw new ValorException(e);
      }
      HttpURLConnection connection = null;
      try {
        connection = (HttpURLConnection) url.openConnection();
        for (Map.Entry<String, String> header : headers.entrySet()) {
          connection.setRequestProperty(header.getKey(), header.getValue());
        }
        try (InputStream is = connection.getInputStream()) {
          return processResponse(is);
        }
      } catch (Exception e) {
        throw new ValorException(e);
      } finally {
        if (connection != null) {
          connection.disconnect();
        }
      }
    }

    protected abstract V processResponse(InputStream is) throws ValorException, IOException;

  }

  class ListRequest extends Request<List<String>> {

    @Override
    protected List<String> processResponse(InputStream is) throws IOException {
      return om.readValue(is, STRING_LIST_TYPE);
    }
  }


  @Override
  protected Collection<String> doGetRelationIds() throws ValorException {
    String path = String.format("%s/relations.json", basePath);
    return new ListRequest().request(path);
  }

  @Override
  protected Relation doGetRelation(String relId) throws ValorException {
    String path = String.format("%s/relations/%s/relation.json", basePath, relId);
    Request<Relation> req = new Request<Relation>() {
      @Override
      protected Relation processResponse(InputStream is) {
        return serde.readRelation(relId, is);
      }
    };
    return req.request(path);
  }

  @Override
  protected void doRegisterRelation(Relation relation) throws ValorException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String doDropRelation(String relId) throws ValorException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Collection<Schema> doGetSchemas(String relationId) throws ValorException {
    Relation relation = findRelation(relationId);
    String path = String.format("%s/relations/%s/schemas.json", basePath, relationId);
    List<String> schemaIds;
    try {
      schemaIds = new ListRequest().request(path);
    } catch (ValorException e) {
      if (e.getCause() instanceof FileNotFoundException) {
        return Collections.EMPTY_LIST;
      }
      throw e;
    }
    List<Schema> schemas = new ArrayList<>(schemaIds.size());
    for (String schemaId : schemaIds) {
      SchemaDescriptor descriptor = getSchemaDescriptor(relationId, schemaId);
      schemas.add(context.buildSchmea(relation, descriptor));
    }
    return schemas;
  }

  @Override
  protected Schema doGetSchema(String relationId, String schemaId) throws ValorException {
    Relation relation = findRelation(relationId);
    SchemaDescriptor schemaDescriptor = getSchemaDescriptor(relationId, schemaId);
    return context.buildSchmea(relation, schemaDescriptor);
  }

  private SchemaDescriptor getSchemaDescriptor(String relationId, String schemaId)
      throws ValorException {
    Request<SchemaDescriptor> schemaReq = new Request<SchemaDescriptor>() {
      @Override
      protected SchemaDescriptor processResponse(InputStream is) throws IOException {
        return serde.readSchema(schemaId, is);
      }
    };
    String path = String.format("%s/relations/%s/schemas/%s.json", basePath, relationId, schemaId);
    return schemaReq.request(path);
  }

  @Override
  protected void doCreateSchema(String relId, SchemaDescriptor schema) throws ValorException {
    throw new UnsupportedOperationException();
  }

  @Override
  protected String doDropSchema(String relId, String schemaId) throws ValorException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
  }

  public static class Factory implements SchemaRepositoryFactory {
    @Override
    public SchemaRepository create(ValorConf conf) {
      try {
        return new HttpsSchemaRepository(conf);
      } catch (ValorException e) {
        throw new IllegalArgumentException(e);
      }
    }

    @Override
    public String getName() {
      return NAME;
    }

    @Override
    public Class<? extends SchemaRepository> getProvidedClass() {
      return HttpsSchemaRepository.class;
    }
  }
}
