package jp.co.cyberagent.valor.spi.storage;

import java.io.IOException;
import jp.co.cyberagent.valor.spi.exception.ValorException;

public interface StorageMutation {

  void execute(StorageConnection conn) throws IOException, ValorException;
}
