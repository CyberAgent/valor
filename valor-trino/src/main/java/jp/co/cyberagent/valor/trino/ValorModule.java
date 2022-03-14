package jp.co.cyberagent.valor.trino;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.analyzer.TypeSignatureTranslator;
import java.util.Collections;
import java.util.Map;
import jp.co.cyberagent.valor.spi.ValorConf;
import jp.co.cyberagent.valor.spi.ValorContext;

public class ValorModule implements Module {

  private final ValorConnectorId connectorId;
  private final TypeManager typeManager;
  private final ValorContext context;
  private final Map<String, ValorConf> nameSpaceConfigs;

  public ValorModule(String connectorId, TypeManager typeManager, ValorContext context) {
    this(connectorId, typeManager, context, Collections.EMPTY_MAP);
  }

  public ValorModule(String connectorId, TypeManager typeManager,
                     ValorContext context, Map<String, ValorConf> nsConf) {
    this.connectorId = new ValorConnectorId(connectorId);
    this.typeManager = typeManager;
    this.context = context;
    this.nameSpaceConfigs = nsConf;
  }

  @Override
  public void configure(Binder binder) {
    binder.bind(ValorConnectorId.class).toInstance(connectorId);
    binder.bind(TypeManager.class).toInstance(typeManager);
    binder.bind(ValorContext.class).toInstance(context);
    binder.bind(ValorConnector.class).in(Scopes.SINGLETON);
    binder.bind(ValorSplitManager.class).in(Scopes.SINGLETON);
    binder.bind(ValorMetadata.class).in(Scopes.SINGLETON);
    binder.bind(ValorRecordSetProvider.class).in(Scopes.SINGLETON);
    binder.bind(ConnectorPageSinkProvider.class).to(ValorPageSinkProvider.class)
        .in(Scopes.SINGLETON);
    binder.bind(ConnectorPageSourceProvider.class).to(ValorPageSourceProvider.class)
        .in(Scopes.SINGLETON);
    binder.bind(new TypeLiteral<Map<String, ValorConf>>(){}).toInstance(nameSpaceConfigs);
  }

  public static final class TypeDeserializer extends FromStringDeserializer<Type> {
    private final TypeManager typeManager;

    @Inject
    public TypeDeserializer(TypeManager typeManager) {
      super(Type.class);
      this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    protected Type _deserialize(String value, DeserializationContext context) {
      Type type = typeManager.getType(
          TypeSignatureTranslator.parseTypeSignature(value, Collections.emptySet()));
      checkArgument(type != null, "Unknown type %s", value);
      return type;
    }
  }
}
