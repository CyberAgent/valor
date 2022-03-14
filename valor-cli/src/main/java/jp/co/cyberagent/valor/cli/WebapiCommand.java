package jp.co.cyberagent.valor.cli;

import java.util.Map;
import jp.co.cyberagent.valor.spi.exception.ValorException;
import jp.co.cyberagent.valor.webapi.Bootstrap;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

public class WebapiCommand implements Command {

  @Override
  public int execute(Map<String, String> conf) throws ValorException {
    String[] args = new String[conf.size()];
    int i = 0;
    for (Map.Entry<String, String> e : conf.entrySet()) {
      args[i++] = String.format("%s=%s", e.getKey(), e.getValue());
    }
    ConfigurableApplicationContext context = SpringApplication.run(Bootstrap.class, args);
    while (context.isActive()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new ValorException(e);
      }
    }
    return SpringApplication.exit(context);
  }

}
