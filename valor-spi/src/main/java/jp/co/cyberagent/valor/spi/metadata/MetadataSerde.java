package jp.co.cyberagent.valor.spi.metadata;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import jp.co.cyberagent.valor.spi.exception.SerdeException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;

public interface MetadataSerde {

  byte[] serialize(Relation relation) throws SerdeException;

  byte[] serialize(SchemaDescriptor schema) throws SerdeException;

  default Relation readRelation(String relationId, byte[] input) throws SerdeException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(input)) {
      return readRelation(relationId, bais);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  default Relation readRelation(String relationId, InputStream is) throws SerdeException {
    try (Reader reader = new InputStreamReader(is)) {
      return readRelation(relationId, reader);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  Relation readRelation(String relationId, Reader reader);

  default SchemaDescriptor readSchema(String schemaId, byte[] input) throws SerdeException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(input)) {
      return readSchema(schemaId, bais);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  default SchemaDescriptor readSchema(String schemaId, InputStream is) throws SerdeException {
    try (Reader reader = new InputStreamReader(is)) {
      return readSchema(schemaId, reader);
    } catch (IOException e) {
      throw new SerdeException(e);
    }
  }

  SchemaDescriptor readSchema(String schemaId, Reader reader) throws SerdeException;

}
