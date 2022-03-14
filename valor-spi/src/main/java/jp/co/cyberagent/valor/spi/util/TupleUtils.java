package jp.co.cyberagent.valor.spi.util;

import java.util.Map;
import java.util.Objects;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;

public class TupleUtils {

  public static Tuple deepMerge(Tuple t1, Tuple t2) {
    Tuple n = t2.getCopy();
    for (String attr : t1.getAttributeNames()) {
      final Object v1 = t1.getAttribute(attr);
      final Object v2 = n.getAttribute(attr);
      if (v2 == null) {
        n.setAttribute(attr, v1);
      } else if (v2 instanceof Map) {
        Map<String, Object> m = deepMerge((Map<String, Object>) v1, (Map<String, Object>) v2);
        n.setAttribute(attr, m);
      }
    }
    return n;
  }

  public static <V> Map<String, V> deepMerge(Map<String, V> m1, Map<String, V> m2) {
    for (Map.Entry<String, V> e : m2.entrySet()) {
      String key = e.getKey();
      Object v = e.getValue();
      if (e.getValue() instanceof Map) {
        v = deepMerge((Map<String, V>) m1.get(key), (Map<String, V>) e.getValue());
      }
      m1.put(key, (V) v);
    }
    return m1;
  }

  public static boolean haveSameKey(Relation relation, Map<String, Object> values, Map<String,
      Object> nextValues) {
    for (String attr : relation.getKeyAttributeNames()) {
      Object current = values.get(attr);
      Object next = nextValues.get(attr);
      if (!Objects.equals(current, next)) {
        return false;
      }
    }
    return true;
  }
}
