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

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.schema.GroupType;

final class JsonGroupConverter extends GroupConverter {

  private final GroupType schema;
  private final Consumer<JsonValue> consumer;
  private final Converter[] converters;

  private JsonObject value;

  // TODO: add support to repeted groups and logical types
  JsonGroupConverter(GroupType schema, Consumer<JsonValue> consumer) {
    this.schema = requireNonNull(schema);
    this.consumer = requireNonNull(consumer);
    this.converters = new Converter[schema.getFieldCount()];
    for (var fieldType : schema.getFields()) {
      String fieldName = fieldType.getName();
      if (fieldType.isPrimitive()) {
        var converter = switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
          case INT32 -> intConverter(v -> value.set(fieldName, v));
          case INT64 -> longConverter(v -> value.set(fieldName, v));
          case FLOAT -> floatConverter(v -> value.set(fieldName, v));
          case DOUBLE -> doubleConverter(v -> value.set(fieldName, v));
          case BOOLEAN -> booleanConverter(v -> value.set(fieldName, v));
          case BINARY -> stringConverter(v -> value.set(fieldName, v));
          default -> throw new UnsupportedOperationException("not supported type: " + fieldType);
        };
        converters[schema.getFieldIndex(fieldName)] = converter;
      } else {
        converters[schema.getFieldIndex(fieldName)] =
          new JsonGroupConverter(fieldType.asGroupType(), v -> value.set(fieldName, v));
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
    initFieldsWithNull();
  }

  private void initFieldsWithNull() {
    for (var fieldType : schema.getFields()) {
      value.add(fieldType.getName(), Json.NULL);
    }
  }

  @Override
  public void end() {
    consumer.accept(value);
    value = null;
  }
}
