package jp.co.cyberagent.valor.sdk;

import jp.co.cyberagent.valor.sdk.io.ValorConnectionImpl;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;

public class ValorConnectionFactory {

  public static ValorConnection create(ValorContext context) {
    return new ValorConnectionImpl(context);
  }

}
