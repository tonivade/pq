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
import org.apache.parquet.schema.LogicalTypeAnnotation;
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
            case INT32 -> intConverter(value.addInt(fieldName));
            case INT64 -> longConverter(value.addLong(fieldName));
            case FLOAT -> floatConverter(value.addFloat(fieldName));
            case DOUBLE -> doubleConverter(value.addDouble(fieldName));
            case BOOLEAN -> booleanConverter(value.addBoolean(fieldName));
            case BINARY -> stringConverter(value.addString(fieldName));
            default -> throw new UnsupportedOperationException("not supported type: " + fieldType);
          };
          converters[schema.getFieldIndex(fieldName)] = converter;
        } else {
          converters[schema.getFieldIndex(fieldName)] =
            new JsonGroupConverter(fieldType.asGroupType(), value.addValue(fieldName));
        }
      } else if (fieldType.isPrimitive()) {
        var converter = switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
          case INT32 -> intConverter(value.setInt(fieldName));
          case INT64 -> longConverter(value.setLong(fieldName));
          case FLOAT -> floatConverter(value.setFloat(fieldName));
          case DOUBLE -> doubleConverter(value.setDouble(fieldName));
          case BOOLEAN -> booleanConverter(value.setBoolean(fieldName));
          case BINARY -> stringConverter(value.setString(fieldName));
          default -> throw new UnsupportedOperationException("not supported type: " + fieldType);
        };
        converters[schema.getFieldIndex(fieldName)] = converter;
      } else {
        var groupType = fieldType.asGroupType();

        if (groupType.getLogicalTypeAnnotation() != null && groupType.getLogicalTypeAnnotation().equals(LogicalTypeAnnotation.listType())) {
          converters[schema.getFieldIndex(fieldName)] = new JsonListConverter(groupType, value.setValue(fieldName));
        } else {
          converters[schema.getFieldIndex(fieldName)] =
              new JsonGroupConverter(groupType, value.setValue(fieldName));
        }
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