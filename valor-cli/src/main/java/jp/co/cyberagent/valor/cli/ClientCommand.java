package jp.co.cyberagent.valor.cli;

import java.util.Map;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.ValorConnectionFactory;
import jp.co.cyberagent.valor.spi.ValorConnection;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;

public interface ClientCommand extends Command {

  @Override
  default int execute(Map<String, String> conf) {
    ValorContext context = StandardContextFactory.create(new ValorConfImpl(conf));
    context.loadPlugins();
    try (ValorConnection client = ValorConnectionFactory.create(context)) {
      return execute(client);
    } catch (Exception e) {
      e.printStackTrace();
      return 1;
    }
  }

  int execute(ValorConnection connection) throws Exception;
}
