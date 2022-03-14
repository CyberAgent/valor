package jp.co.cyberagent.valor.hive;

import static org.junit.jupiter.api.Assertions.assertEquals;


import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;
import jp.co.cyberagent.valor.spi.schema.scanner.filter.CompleteMatchSegment;
import jp.co.cyberagent.valor.spi.storage.StorageScan;
import jp.co.cyberagent.valor.spi.storage.StorageScanImpl;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

public class TestValorSplit {

  @Test
  public void testWriteAndRead() throws Exception {
    List<String> fields = Arrays.asList("key", "val");
    FieldComparator compartor = new FieldComparator();
    compartor.append(new CompleteMatchSegment(null, new byte[]{0x00}));
    Map<String, FieldComparator> comparators = Collections.singletonMap("key", compartor);
    StorageScan scan = new StorageScanImpl(fields, comparators);

    ValorSplit split = new ValorSplit(new Path("dummyPath"), "relId", "schemaId", scan);
    try (
        PipedInputStream pis = new PipedInputStream();
        PipedOutputStream pos = new PipedOutputStream(pis);
        DataOutputStream dos = new DataOutputStream(pos);
        DataInputStream dis = new DataInputStream(pis)
    ) {
      split.write(dos);
      dos.flush();
      ValorSplit reversed = new ValorSplit();
      reversed.readFields(dis);
      assertEquals(split, reversed);
    }

  }

}
