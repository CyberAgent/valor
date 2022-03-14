package jp.co.cyberagent.valor.trino;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.block.Block;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeUtils;
import io.trino.spi.type.VarcharType;
import jp.co.cyberagent.valor.sdk.formatter.Murmur3MapKeyFormatter;

public class Murmur3Hash {

  @ScalarFunction("murmur3_binary")
  @Description("murmur3 hash in binary from array")
  @TypeParameter("E")
  @SqlType(StandardTypes.VARBINARY)
  public static Slice murmur3Bytes(
      @TypeParameter("E") Type elementType,
      @SqlType("array(E)") Block block,
      @SqlType(StandardTypes.INTEGER) long length
  ) {
    ImmutableList.Builder<Object> arrayBuilder = ImmutableList.builder();
    for (int i = 0; i < block.getPositionCount(); ++i) {
      if (elementType.getJavaType() == Slice.class) {
        Slice slice = (Slice) requireNonNull(TypeUtils.readNativeValue(elementType, block, i));
        arrayBuilder.add(
            (elementType instanceof VarcharType) ? slice.toStringUtf8() : slice.getBytes());
      } else {
        arrayBuilder.add(TypeUtils.readNativeValue(elementType, block, i));
      }
    }
    ImmutableList<Object> array = arrayBuilder.build();
    byte[] hashBytes = Murmur3MapKeyFormatter.toHashBytes(
        array, ValorTrinoUtil.toValorType(elementType), (int)length);
    return Slices.wrappedBuffer(hashBytes);
  }

}
