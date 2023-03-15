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

final class JsonListConverter extends GroupConverter {

  private final Consumer<JsonValue> consumer;
  private final JsonArrayHolder value = new JsonArrayHolder();
  private final Converter converter;

  JsonListConverter(GroupType schema, Consumer<JsonValue> consumer) {
    this.consumer = requireNonNull(consumer);

    var fieldType = schema.getFields().get(0);
    if (fieldType.isPrimitive()) {
      this.converter = switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
        case INT32 -> intConverter(value.addInt());
        case INT64 -> longConverter(value.addLong());
        case FLOAT -> floatConverter(value.addFloat());
        case DOUBLE -> doubleConverter(value.addDouble());
        case BOOLEAN -> booleanConverter(value.addBoolean());
        case BINARY -> stringConverter(value.addString());
        default -> throw new UnsupportedOperationException("not supported type: " + fieldType);
      };
    } else {
      converter = new JsonGroupConverter(schema.getFields().get(0).asGroupType(), value.addValue());
    }
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converter;
  }

  @Override
  public void start() {
    value.create();
  }

  @Override
  public void end() {
    value.accept(consumer);
  }

}
