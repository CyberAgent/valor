package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import jp.co.cyberagent.valor.sdk.serde.tree.RecordTreeNode;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeBasedTupleSerializer;
import jp.co.cyberagent.valor.sdk.serde.tree.TreeNode;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.LongAttributeType;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestMultiAttributeValuesFormatter {

  @Mock private Relation relation;

  @BeforeEach
  public void initMock() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testExclusion() throws Exception {
    when(relation.getAttributeType(any())).thenReturn(StringAttributeType.INSTANCE);
    MultiAttributeValuesFormatter element = MultiAttributeValuesFormatter.create("k1");
    TupleImpl sample = new TupleImpl(relation);
    sample.setAttribute("k1", "v1");
    sample.setAttribute("k2", "v2");

    byte[] expected = ByteUtils.toBytes("v2");

    TreeBasedTupleSerializer serializer = mock(TreeBasedTupleSerializer.class);
    TreeNode<byte[]> parent =
        new RecordTreeNode("", TreeNode.ATTRIBUTE_NAME_TYPE_PREFIX, ByteUtils.toBytes("k2"));
    when(serializer.getCurrentParent()).thenReturn(parent);
    element.accept(serializer, sample);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }

  @Test
  public void testCustomFormatter() throws Exception {
    when(relation.getAttributeType(any())).thenReturn(LongAttributeType.INSTANCE);
    when(relation.getAttributeNames()).thenReturn(ImmutableList.of("k1", "k2"));
    MultiAttributeValuesFormatter element =
        new MultiAttributeValuesFormatter.Factory()
            .create(
                ImmutableList.of("k1"),
                ImmutableMap.of("k2", new ReverseLongFormatter.Factory().create("k2")));
    TupleImpl sample = new TupleImpl(relation);
    sample.setAttribute("k1", "v1");
    Long v2 = 1L;
    sample.setAttribute("k2", v2);
    sample.setAttribute("k3", v2); // Redundant invalid attributes.

    byte[] expected = ByteUtils.toBytes(Long.MAX_VALUE - v2);

    TreeBasedTupleSerializer serializer = mock(TreeBasedTupleSerializer.class);
    TreeNode<byte[]> parent =
        new RecordTreeNode("", TreeNode.ATTRIBUTE_NAME_TYPE_PREFIX, ByteUtils.toBytes("k2"));
    when(serializer.getCurrentParent()).thenReturn(parent);
    element.accept(serializer, sample);
    verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
  }
}
