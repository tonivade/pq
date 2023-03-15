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
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
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

  private JsonObject value;

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
            case INT32 -> intConverter(i -> {
              if (value.get(fieldName).isNull()) {
                value.set(fieldName, new JsonArray());
              }
              value.get(fieldName).asArray().add(i);
            });
            case INT64 -> longConverter(l -> {
              if (value.get(fieldName).isNull()) {
                value.set(fieldName, new JsonArray());
              }
              value.get(fieldName).asArray().add(l);
            });
            case FLOAT -> floatConverter(f -> {
              if (value.get(fieldName).isNull()) {
                value.set(fieldName, new JsonArray());
              }
              value.get(fieldName).asArray().add(f);
            });
            case DOUBLE -> doubleConverter(d -> {
              if (value.get(fieldName).isNull()) {
                value.set(fieldName, new JsonArray());
              }
              value.get(fieldName).asArray().add(d);
            });
            case BOOLEAN -> booleanConverter(b -> {
              if (value.get(fieldName).isNull()) {
                value.set(fieldName, new JsonArray());
              }
              value.get(fieldName).asArray().add(b);
            });
            case BINARY -> stringConverter(s -> {
              if (value.get(fieldName).isNull()) {
                value.set(fieldName, new JsonArray());
              }
              value.get(fieldName).asArray().add(s);
            });
            default -> throw new UnsupportedOperationException("not supported type: " + fieldType);
          };
          converters[schema.getFieldIndex(fieldName)] = converter;
        } else {
          converters[schema.getFieldIndex(fieldName)] =
            new JsonGroupConverter(fieldType.asGroupType(), v -> {
              if (value.get(fieldName).isNull()) {
                value.set(fieldName, new JsonArray());
              }
              value.get(fieldName).asArray().add(v);
            });
        }
      } else if (fieldType.isPrimitive()) {
        var converter = switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
          case INT32 -> intConverter(i -> value.set(fieldName, i));
          case INT64 -> longConverter(l -> value.set(fieldName, l));
          case FLOAT -> floatConverter(f -> value.set(fieldName, f));
          case DOUBLE -> doubleConverter(d -> value.set(fieldName, d));
          case BOOLEAN -> booleanConverter(b -> value.set(fieldName, b));
          case BINARY -> stringConverter(s -> value.set(fieldName, s));
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

  @Override
  public void end() {
    consumer.accept(value);
    value = null;
  }

  private void initFieldsWithNull() {
    for (var fieldType : schema.getFields()) {
      value.add(fieldType.getName(), Json.NULL);
    }
  }
}
