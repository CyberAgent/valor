package jp.co.cyberagent.valor.sdk.formatter;

import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeNode;
import jp.co.cyberagent.valor.spi.exception.MismatchByteArrayException;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class TestMultiAttributeNamesFormatter {

  private MultiAttributeNamesFormatter element;

  @Test
  public void testExclusion() throws MismatchByteArrayException {
    this.element = MultiAttributeNamesFormatter.create("k1");
    TupleImpl sample = new TupleImpl(null);
    sample.setAttribute("k1", "v1");
    sample.setAttribute("k2", "v2");

    byte[] expected = ByteUtils.toBytes("k2");

    TupleSerializer serializer = mock(TreeBasedTupleSerializer.class);
    element.accept(serializer, sample);
    verify(serializer).write(eq(TreeNode.ATTRIBUTE_NAME_TYPE_PREFIX),
        argThat(equalBytes(expected)));
  }
}
