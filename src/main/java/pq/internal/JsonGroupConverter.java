package pq.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

public final class JsonGroupConverter extends GroupConverter {
  
  private final GroupType schema;
  private final Consumer<JsonValue> consumer;
  private final Map<String, Converter> converters = new HashMap<>();

  private JsonObject value;
  
  public JsonGroupConverter(GroupType schema, Consumer<JsonValue> consumer) {
    this.schema = Objects.requireNonNull(schema);
    this.consumer = consumer;
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    Type fieldType = getFieldType(fieldIndex);
    String fieldName = fieldType.getName();
    if (fieldType.isPrimitive()) {
      var converter = switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
        case INT32 -> new JsonPrimitiveConverterFactory.IntegerConverter(v -> value.add(fieldName, v));
        case INT64 -> new JsonPrimitiveConverterFactory.LongConverter(v -> value.add(fieldName, v));
        case FLOAT -> new JsonPrimitiveConverterFactory.FloatConverter(v -> value.add(fieldName, v));
        case DOUBLE -> new JsonPrimitiveConverterFactory.DoubleConverter(v -> value.add(fieldName, v));
        case BOOLEAN -> new JsonPrimitiveConverterFactory.BooleanConverter(v -> value.add(fieldName, v));
        case BINARY -> new JsonPrimitiveConverterFactory.StringConverter(v -> value.add(fieldName, v));
        default -> throw new UnsupportedOperationException("not supported type: " + fieldType);
      };
      converters.put(fieldName, converter);
      return converter;  
    } else {
      var converter = new JsonGroupConverter(fieldType.asGroupType(), v -> value.add(fieldName, v));
      converters.put(fieldName, converter);
      return converter;  
    }
  }

  @Override
  public void start() {
    value = new JsonObject();
  }

  @Override
  public void end() {
    consumer.accept(value);
  }
  
  private Type getFieldType(int fieldIndex) {
    return schema.getFields().get(fieldIndex);
  }

}
