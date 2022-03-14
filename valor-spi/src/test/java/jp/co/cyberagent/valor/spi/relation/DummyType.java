package jp.co.cyberagent.valor.spi.relation;

import java.io.DataOutput;
import java.util.Collections;
import java.util.List;
import jp.co.cyberagent.valor.spi.exception.IllegalTypeException;
import jp.co.cyberagent.valor.spi.relation.DummyType.Dummy;
import jp.co.cyberagent.valor.spi.relation.type.AttributeType;

@SuppressWarnings("rawtypes")
public class DummyType extends AttributeType<Dummy> {

  @Override
  protected int doWrite(DataOutput out, Object o) throws IllegalTypeException {
    return 0;
  }

  @Override
  public Dummy doRead(byte[] in, int offset, int length) throws IllegalTypeException {
    return new Dummy();
  }

  @Override
  public int getSize() {
    return 0;
  }

  @Override
  public Class<Dummy> getRepresentedClass() {
    return Dummy.class;
  }

  @Override
  public List<AttributeType> getGenericParameterValues() {
    return Collections.emptyList();
  }

  @Override
  public void addGenericElementType(AttributeType attributeType) {
    throw new UnsupportedOperationException("addGenericElementType");
  }

  @Override
  public String getName() {
    return "dummy";
  }

  public static class Dummy {

  }
}
