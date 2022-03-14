package jp.co.cyberagent.valor.webapi;

import java.util.Map;
import java.util.stream.Collectors;
import jp.co.cyberagent.valor.sdk.StandardContextFactory;
import jp.co.cyberagent.valor.sdk.metadata.MetadataJsonSerde;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;
import jp.co.cyberagent.valor.spi.conf.ValorConfImpl;
import jp.co.cyberagent.valor.spi.metadata.SchemaRepository;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

@SpringBootApplication
@ComponentScan(basePackages = {"jp.co.cyberagent.valor.webapi.controller"})
public class Bootstrap {

  private ValorContext context;

  private SchemaRepository repository;

  private MetadataJsonSerde jsonSerde;

  public Bootstrap(ApplicationArguments args) {
    args.getNonOptionArgs().forEach(System.out::println);
    Map<String, String> conf = args.getNonOptionArgs().stream()
        .map(e -> e.split("="))
        .collect(Collectors.toMap(e -> e[0], e -> e[1]));
    ValorConf valorConf = new ValorConfImpl(conf);
    context = StandardContextFactory.create(valorConf);
    repository = context.createRepository(valorConf);
    jsonSerde = new MetadataJsonSerde(context);
  }

  @Bean
  public ValorContext getValorContext() {
    return context;
  }

  @Bean
  public SchemaRepository getSchemaRepository() {
    return repository;
  }

  @Bean
  public MetadataJsonSerde getJsonSerde() {
    return jsonSerde;
  }

  @Bean
  public Docket petApi() {
    return new Docket(DocumentationType.SWAGGER_2)
        .select()
        .paths(PathSelectors.any())
        .build()
        .apiInfo(apiInfo());
  }

  private ApiInfo apiInfo() {
    return new ApiInfo(
        "valor webapi",
        "valor schema management web api",
        "0.1.0",
        "",
        "",
        "",
        ""
    );
  }

  public static void main(String[] args) {
    SpringApplication.run(Bootstrap.class, args);
  }

}
