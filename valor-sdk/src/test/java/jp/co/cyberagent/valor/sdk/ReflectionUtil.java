package jp.co.cyberagent.valor.sdk;

import java.lang.reflect.Field;

public class ReflectionUtil {

  public static void setField(Object obj, String name, Object value) throws ReflectiveOperationException {
    Field field =  obj.getClass().getDeclaredField(name);
    field.setAccessible(true);
    field.set(obj, value);
  }
}
