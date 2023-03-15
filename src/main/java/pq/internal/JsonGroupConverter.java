/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import static java.util.Objects.requireNonNull;
import static pq.internal.JsonPrimitiveConverterFactory.booleanConverter;
import static pq.internal.JsonPrimitiveConverterFactory.doubleConverter;
import static pq.internal.JsonPrimitiveConverterFactory.floatConverter;
import static pq.internal.JsonPrimitiveConverterFactory.intConverter;
import static pq.internal.JsonPrimitiveConverterFactory.longConverter;
import static pq.internal.JsonPrimitiveConverterFactory.stringConverter;

import com.eclipsesource.json.JsonValue;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type.Repetition;

final class JsonGroupConverter extends GroupConverter {

  private final GroupType schema;
  private final Consumer<JsonValue> consumer;
  private final Converter[] converters;

  private JsonObjectHolder value = new JsonObjectHolder();

  // TODO: add support to repeted groups and logical types
  JsonGroupConverter(GroupType schema, Consumer<JsonValue> consumer) {
    this.schema = requireNonNull(schema);
    this.consumer = requireNonNull(consumer);
    this.converters = new Converter[schema.getFieldCount()];
    for (var fieldType : schema.getFields()) {
      String fieldName = fieldType.getName();
      if (fieldType.isRepetition(Repetition.REPEATED)) {
        if (fieldType.isPrimitive()) {
          var converter = switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> intConverter(value.intArrayConsumer(fieldName));
            case INT64 -> longConverter(value.longArrayConsumer(fieldName));
            case FLOAT -> floatConverter(value.floatArrayConsumer(fieldName));
            case DOUBLE -> doubleConverter(value.doubleArrayConsumer(fieldName));
            case BOOLEAN -> booleanConverter(value.booleanArrayConsumer(fieldName));
            case BINARY -> stringConverter(value.stringArrayConsumer(fieldName));
            default -> throw new UnsupportedOperationException("not supported type: " + fieldType);
          };
          converters[schema.getFieldIndex(fieldName)] = converter;
        } else {
          converters[schema.getFieldIndex(fieldName)] =
            new JsonGroupConverter(fieldType.asGroupType(), value.valueArrayConsumer(fieldName));
        }
      } else if (fieldType.isPrimitive()) {
        var converter = switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
          case INT32 -> intConverter(value.intConsumer(fieldName));
          case INT64 -> longConverter(value.longConsumer(fieldName));
          case FLOAT -> floatConverter(value.floatConsumer(fieldName));
          case DOUBLE -> doubleConverter(value.doubleConsumer(fieldName));
          case BOOLEAN -> booleanConverter(value.booleanConsumer(fieldName));
          case BINARY -> stringConverter(value.stringConsumer(fieldName));
          default -> throw new UnsupportedOperationException("not supported type: " + fieldType);
        };
        converters[schema.getFieldIndex(fieldName)] = converter;
      } else {
        converters[schema.getFieldIndex(fieldName)] =
          new JsonGroupConverter(fieldType.asGroupType(), value.valueConsumer(fieldName));
      }
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    value.create(schema);
  }

  @Override
  public void end() {
    value.accept(consumer);
  }
}
