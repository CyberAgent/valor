package jp.co.cyberagent.valor.webapi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
public class SchemaController {

  @Autowired
  private SchemaRepository repository;

  @Autowired
  private MetadataJsonSerde jsonSerde;

  // for formatting json
  private ObjectMapper objectMapper = new ObjectMapper();

  @GetMapping(path = "relations")
  public Collection<String> getRelations() throws ValorException {
    return repository.listRelationIds();
  }

  @GetMapping(path = "relations/{id}")
  public String getRelation(@PathVariable String id) throws ValorException {
    Relation relation = repository.findRelation(id);
    if (relation == null) {
      throw new ResponseStatusException(
          HttpStatus.NOT_FOUND, String.format("relation %s is not found", id));
    }
    return new String(jsonSerde.serialize(relation));
  }

  @GetMapping(path = "relations/{id}/schemas")
  private Collection<Object> getSchemas(@PathVariable String id) throws ValorException,
      IOException {
    Collection<Schema> schemas = repository.listSchemas(id);
    if (schemas == null) {
      throw new ResponseStatusException(
          HttpStatus.NOT_FOUND, String.format("schema for %s is not found", id));
    }
    List<Object> result = new ArrayList<>(schemas.size());
    for (Schema s : schemas) {
      byte[] sd = jsonSerde.serialize(SchemaDescriptor.from(s));
      result.add(objectMapper.readValue(sd, Map.class));
    }
    return result;
  }

}
