package jp.co.cyberagent.valor.cli;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.exception.ValorRuntimeException;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import org.kohsuke.args4j.Option;

public class InsertCommand implements ClientCommand {
  @Option(name = "-f", usage = "record definition file (json)")
  private String pathToRecordFile;

  @Option(name = "-r", usage = "relation id", required = true)
  private String relationId;

  @Option(name = "-n", usage = "number of records per insert (default = 100000)")
  private int recordsPerInsert = 100000;

  @Override
  public int execute(ValorConnection client) throws Exception {
    TypeReference<HashMap<String, Object>> reference =
        new TypeReference<HashMap<String, Object>>() {
        };
    ObjectMapper mapper = new ObjectMapper();
    Relation relation = client.findRelation(relationId);
    try (Scanner scanner = buildScanner()) {
      while (scanner.hasNextLine()) {
        List<Tuple> tuples = new ArrayList<>(recordsPerInsert);
        for (int c = 0; c < recordsPerInsert && scanner.hasNextLine(); c++) {
          String line = scanner.nextLine();
          Map<String, Object> map = mapper.readValue(line, reference);
          TupleImpl tuple = new TupleImpl(relation);
          for (String key : map.keySet()) {
            Object v = map.get(key);
            Relation.Attribute type = relation.getAttribute(key);
            v = castValue(key, type, v);
            if (v != null) {
              tuple.setAttribute(key, v);
            }
          }
          tuples.add(tuple);
        }
        client.insert(relationId, tuples);
      }
    }
    return 0;
  }

  private Object castValue(String key, Relation.Attribute attr, Object v) {
    if (attr == null) {
      throw new IllegalArgumentException("unknown attribute: " + key);
    }
    if (v == null) {
      if (attr.isNullable()) {
        return null;
      }
      throw new ValorRuntimeException(attr.name() + " is not nullable");
    }
    Class cls = attr.type().getRepresentedClass();
    if (v instanceof Number) {
      if (cls == Long.class) {
        return  ((Number) v).longValue();
      } else if (cls == Integer.class) {
        return  ((Number) v).intValue();
      } else if (cls == Float.class) {
        return  ((Number) v).floatValue();
      } else if (cls == Double.class) {
        return  ((Number) v).doubleValue();
      } else if (cls == Number.class) {
        if (v instanceof Float || v instanceof Double) {
          return  ((Number) v).floatValue();
        } else {
          return  ((Number) v).longValue();
        }
      } else {
        throw new IllegalArgumentException(String.format(
            "number type mismatch: expected %s but %s is %s",
            cls.getName(), v, v.getClass()));
      }
    } else if (cls.isInstance(v)) {
      return v;
    } else {
      throw new IllegalArgumentException(String.format(
          "type mismatch %s : expected %s but %s is %s",
          key, cls.getName(), v, v == null ? "null" : v.getClass().getCanonicalName()));
    }
  }

  private Scanner buildScanner() throws FileNotFoundException {
    if (pathToRecordFile != null) {
      return new Scanner(new File(pathToRecordFile));
    } else {
      return new Scanner(System.in);
    }
  }

}
