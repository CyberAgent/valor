package jp.co.cyberagent.valor.sdk.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.conf.ValorConfParam;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepositoryFactory;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.schema.Schema;
import jp.co.cyberagent.valor.spi.schema.SchemaDescriptor;
import jp.co.cyberagent.valor.spi.storage.Storage;

/**
 *
 */
public class FileSchemaRepository extends SchemaRepositoryBase {
  public static final ValorConfParam SCHEMA_REPOS_BASEDIR = new ValorConfParam(
      "valor.schemarepository.file.basedir", "/etc/valor/relations");

  public static final String NAME = "file";

  private static final String SCHEMAS_DIR = "schemas";

  private File baseDir;

  protected FileSchemaRepository(ValorConf conf) throws ValorException {
    super(conf);
    baseDir = new File(SCHEMA_REPOS_BASEDIR.get(conf));
    doGetRelationIds();
  }

  @Override
  protected Collection<String> doGetRelationIds() throws ValorException {
    Collection<String> relIds = new HashSet<>();
    for (File dir : baseDir.listFiles(f -> f.isDirectory() && !f.getName().startsWith("."))) {
      String relationId = dir.getName();
      relIds.add(relationId);
    }
    return relIds;
  }

  @Override
  protected Relation doGetRelation(String relId) throws ValorException {
    File file = relationDefFile(relId);
    if (!file.exists()) {
      return null;
    }
    try (InputStream is = new FileInputStream(file)) {
      return serde.readRelation(relId, is);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected void doRegisterRelation(Relation relation) throws ValorException {
    File f = relationDefFile(relation.getRelationId());
    File d = new File(f.getParent());
    d.mkdirs();
    try (OutputStream os = new FileOutputStream(f)) {
      byte[] v = serde.serialize(relation);
      os.write(v);
      os.flush();
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected String doDropRelation(String relId) throws ValorException {
    File dir = relationDefFile(relId).getParentFile();
    if (dir.exists()) {
      File backUpDir = new File(dir.getParentFile(), "." + dir.getName());
      if (backUpDir.exists()) {
        try {
          Files.walk(backUpDir.toPath())
              .sorted(Comparator.reverseOrder())
              .map(Path::toFile)
              .forEach(File::delete);
        } catch (IOException e) {
          throw new ValorException("failed to remove old backup dir " + backUpDir, e);
        }
      }
      dir.renameTo(backUpDir);
      return relId;
    }
    return null;
  }

  @Override
  protected Collection<Schema> doGetSchemas(String relationId) throws ValorException {
    Relation relation = doGetRelation(relationId);
    File d = schemaDefDir(relationId);
    if (!d.exists()) {
      return Collections.emptySet();
    }
    Collection<Schema> schemas = new ArrayList<>();
    for (File s : d.listFiles(f -> f.isFile() && f.getName().endsWith(".json"))) {
      String schemaId = s.getName();
      schemaId = schemaId.substring(0, schemaId.lastIndexOf("."));
      try (InputStream is = new FileInputStream(s)) {
        SchemaDescriptor descriptor = serde.readSchema(schemaId, is);
        Storage storage = getContext().createStorage(descriptor);
        schemas.add(storage.buildSchema(relation, descriptor));
      } catch (IOException e) {
        throw new ValorException(e);
      }
    }
    return schemas;
  }

  @Override
  protected Schema doGetSchema(String relationId, String schemaId) throws ValorException {
    Relation relation = doGetRelation(relationId);
    File f = schemaDefFile(relationId, schemaId);
    try (InputStream is = new FileInputStream(f)) {
      SchemaDescriptor descriptor = serde.readSchema(schemaId, is);
      Storage storage = getContext().createStorage(descriptor);
      return storage.buildSchema(relation, descriptor);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected void doCreateSchema(String relId, SchemaDescriptor schemaDescriptor)
      throws ValorException {
    String schemaId = schemaDescriptor.getSchemaId();
    File f = schemaDefDir(relId);
    f.mkdirs();
    f = schemaDefFile(relId, schemaId);
    if (f.exists()) {
      File backup = Paths.get(
          baseDir.getPath(), relId, SCHEMAS_DIR, f.getName() + ".bak").toFile();
      f.renameTo(backup);
    }
    try (OutputStream os = new FileOutputStream(f)) {
      byte[] val = serde.serialize(schemaDescriptor);
      os.write(val);
    } catch (IOException e) {
      throw new ValorException(e);
    }
  }

  @Override
  protected String doDropSchema(String relId, String schemaId) throws ValorException {
    File f = schemaDefFile(relId, schemaId);
    if (f.exists()) {
      File backup = Paths.get(
          baseDir.getPath(), relId, SCHEMAS_DIR, f.getName() + ".bak").toFile();
      f.renameTo(backup);
      return schemaId;
    }
    return null;
  }

  @Override
  public void close() {
  }

  private File relationDefFile(String relId) {
    return Paths.get(baseDir.getAbsolutePath(), relId, "relation.json").toFile();
  }

  private File schemaDefDir(String relId) {
    return Paths.get(baseDir.getAbsolutePath(), relId, "schemas").toFile();
  }

  private File schemaDefFile(String relId, String schemaId) {
    return new File(schemaDefDir(relId), schemaId + ".json");
  }

  public static class Factory implements SchemaRepositoryFactory {
    @Override
    public SchemaRepository create(ValorConf conf) {
      try {
        return new FileSchemaRepository(conf);
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
      return FileSchemaRepository.class;
    }
  }
}
