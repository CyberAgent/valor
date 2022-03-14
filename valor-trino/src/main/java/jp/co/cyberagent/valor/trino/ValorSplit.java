package jp.co.cyberagent.valor.trino;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import jp.co.cyberagent.valor.spi.schema.FieldComparator;

/**
 *
 */
public class ValorSplit implements ConnectorSplit {

  static Base64.Encoder encoder = Base64.getEncoder();
  static Base64.Decoder decoder = Base64.getDecoder();

  private String connectorId;
  private SchemaTableName schemaTableName;
  private String schemaExp;
  private TupleDomain<ColumnHandle> constraint;
  private Map<String, FieldComparatorDesc> comparators;

  @JsonCreator
  public ValorSplit(@JsonProperty("connectorId") String connectorId,
                    @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
                    @JsonProperty("schemaExp") String schemaExp,
                    @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
                    @JsonProperty("comparators") Map<String, FieldComparatorDesc> comparators) {
    this.connectorId = connectorId;
    this.schemaTableName = schemaTableName;
    this.schemaExp = schemaExp;
    this.constraint = constraint;
    this.comparators = comparators;
  }

  @JsonProperty
  public String getConnectorId() {
    return this.connectorId;
  }

  @JsonProperty
  public SchemaTableName getSchemaTableName() {
    return schemaTableName;
  }

  @JsonProperty
  public String getSchemaExp() {
    return schemaExp;
  }

  @JsonProperty
  public TupleDomain<ColumnHandle> getConstraint() {
    return constraint;
  }

  @JsonProperty
  public Map<String, FieldComparatorDesc> getComparators() {
    return comparators;
  }

  @Override
  public boolean isRemotelyAccessible() {
    return true;
  }

  @Override
  public List<HostAddress> getAddresses() {
    return ImmutableList.of();
  }

  @Override
  public Object getInfo() {
    return this;
  }

  @Override
  public String toString() {
    StringBuilder compExp = new StringBuilder();
    for (Map.Entry<String, FieldComparatorDesc> c : comparators.entrySet()) {
      compExp.append(c.getKey()).append("=").append(c.getValue()).append(", ");
    }
    return String.format("%s[%s](%s)", schemaTableName, schemaExp, compExp.toString());
  }

  public static class FieldComparatorDesc {
    private FieldComparator.Operator operator;
    private String prefix;
    private String start;
    private String stop;
    private String regexp;

    @JsonCreator
    public FieldComparatorDesc(@JsonProperty("operator") FieldComparator.Operator operator,
                               @JsonProperty("prefix") String prefix,
                               @JsonProperty("start") String start,
                               @JsonProperty("stop") String stop,
                               @JsonProperty("regexp") String regexp) {
      this.operator = operator;
      this.prefix = prefix;
      this.start = start;
      this.stop = stop;
      this.regexp = regexp;
    }

    public FieldComparatorDesc(FieldComparator.Operator operator, byte[] prefix, byte[] start,
                               byte[] stop, byte[] regexp) {
      this.operator = operator;
      this.prefix = prefix == null ? null : encoder.encodeToString(prefix);
      this.start = start == null ? null : encoder.encodeToString(start);
      this.stop = stop == null ? null : encoder.encodeToString(stop);
      this.regexp = regexp == null ? null : encoder.encodeToString(regexp);
    }

    @JsonProperty
    public FieldComparator.Operator getOperator() {
      return operator;
    }

    @JsonProperty
    public String getPrefix() {
      return prefix;
    }

    @JsonProperty
    public String getStart() {
      return start;
    }

    @JsonProperty
    public String getStop() {
      return stop;
    }

    @JsonProperty
    public String getRegexp() {
      return regexp;
    }

    public byte[] prefix() {
      return prefix == null ? null : decoder.decode(prefix);
    }

    public byte[] start() {
      return start == null ? null : decoder.decode(start);
    }

    public byte[] stop() {
      return stop == null ? null : decoder.decode(stop);
    }

    public byte[] regexp() {
      return regexp == null ? null : decoder.decode(regexp);
    }

    @Override
    public String toString() {
      switch (operator) {
        case HEAD:
          return "";
        case EQUAL:
          return String.format("= %s", start);
        case NOT_EQUAL:
          return String.format("!= %s", start);
        case BETWEEN:
          return String.format("BETWEEN %s AND %s", start, stop);
        case GREATER:
          return String.format(">= %s", start);
        case LESS:
          return String.format("< %s", stop);
        case REGEXP:
          return String.format("LIKE %s", regexp);
        default:
          throw new IllegalArgumentException(operator.name());
      }
    }
  }
}
