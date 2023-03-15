package pq.internal;

import static java.util.Objects.requireNonNull;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

public final class JsonGroupConverter extends GroupConverter {
  
  private final Consumer<JsonValue> consumer;
  private final Converter[] converters;

  private JsonObject value;
  
  public JsonGroupConverter(GroupType schema, Consumer<JsonValue> consumer) {
    this.consumer = requireNonNull(consumer);
    this.converters = new Converter[schema.getFieldCount()];
    for (var fieldType : schema.getFields()) {
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
        converters[schema.getFieldIndex(fieldName)] = converter;
      } else {
        converters[schema.getFieldIndex(fieldName)] = new JsonGroupConverter(fieldType.asGroupType(), v -> value.add(fieldName, v));
      }
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    value = new JsonObject();
  }

  @Override
  public void end() {
    consumer.accept(value);
  }
}
