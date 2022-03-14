package jp.co.cyberagent.valor.sdk.formatter;

import static jp.co.cyberagent.valor.sdk.EqualBytes.equalBytes;
import static jp.co.cyberagent.valor.sdk.ReflectionUtil.setField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.net.URLEncoder;
import jp.co.cyberagent.valor.sdk.serde.OneToOneDeserializer;
import jp.co.cyberagent.valor.spi.relation.ImmutableRelation;
import jp.co.cyberagent.valor.spi.relation.Relation;
import jp.co.cyberagent.valor.spi.relation.Tuple;
import jp.co.cyberagent.valor.spi.relation.TupleImpl;
import jp.co.cyberagent.valor.spi.relation.type.StringAttributeType;
import jp.co.cyberagent.valor.spi.serde.TupleDeserializer;
import jp.co.cyberagent.valor.spi.serde.TupleSerializer;
import jp.co.cyberagent.valor.spi.util.ByteUtils;
import org.junit.jupiter.api.Test;

public class TestUrlEncodeFormatter {

    static final String ATTR_NAME = "attr";

    private UrlEncodeFormatter serde = new UrlEncodeFormatter(ATTR_NAME);
    private Relation relation =
            ImmutableRelation.builder().relationId("test").addAttribute(ATTR_NAME, true,
                    StringAttributeType.INSTANCE).build();

    @Test
    public void testSerialize() throws Exception {
        Tuple t = new TupleImpl(relation);
        t.setAttribute(ATTR_NAME, "てすと");
        byte[] expected = ByteUtils.toBytes(URLEncoder.encode("てすと",StringAttributeType.CHARSET_NAME));

        TupleSerializer serializer = mock(TupleSerializer.class);
        serde.accept(serializer, t);
        verify(serializer).write(isNull(String.class), argThat(equalBytes(expected)));
    }

    @Test
    public void testDeserialize() throws Exception {
        String v = "てすと";
        TupleDeserializer f = new OneToOneDeserializer(relation, "", serde);
        setField(f, "tuple", new TupleImpl(relation));
        byte[] bytes = ByteUtils.toBytes(URLEncoder.encode(v,StringAttributeType.CHARSET_NAME));
        serde.cutAndSet(bytes, 0, bytes.length, f);
        Tuple t = f.pollTuple();
        assertEquals("てすと", t.getAttribute(ATTR_NAME));
    }
}
