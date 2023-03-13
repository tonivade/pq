/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonObject.Member;
import com.eclipsesource.json.JsonValue;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.hadoop.ParquetWriter;

class Writer {

  private final Schema schema;
  private final ParquetWriter<GenericRecord> output;

  Writer(Schema schema, ParquetWriter<GenericRecord> output) {
    this.schema = schema;
    this.output = output;
  }

  void write(JsonValue value) {
    try {
      output.write(toRecord(schema, value));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static GenericRecord toRecord(Schema schema, JsonValue json) {
    if (json instanceof JsonObject object) {
      var value = new GenericData.Record(schema);
      for (Member member : object) {
        Field field = schema.getField(member.getName());
        value.put(member.getName(), convert(field.schema(), member.getValue()));
      }
      return value;
    }
    throw new IllegalStateException();
  }

  private static Object convert(Schema schema, JsonValue json) {
    if (schema.isUnion()) {
      // XXX: I'm not sure how to manage union types
      return convert(schema.getTypes().stream().filter(s -> s.getType() != Type.NULL).findFirst().orElseThrow(), json);
    }
    if (json instanceof JsonObject object) {
      var record = new GenericData.Record(schema);
      for (Member member : object) {
        Field field = schema.getField(member.getName());
        record.put(member.getName(), convert(field.schema(), member.getValue()));
      }
      return record;
    } else if (json instanceof JsonArray jsonArray) {
      var array = new GenericData.Array<>(jsonArray.size(), schema.getElementType());
      for (JsonValue value : jsonArray) {
        array.add(convert(schema.getElementType(), value));
      }
      return array;
    } else if (json.isString()) {
      return json.asString();
    } else if (json.isBoolean()) {
      return json.asBoolean();
    } else if (json.isNumber()) {
      return switch (schema.getType()) {
        case INT -> json.asInt();
        case LONG -> json.asLong();
        case FLOAT -> json.asFloat();
        case DOUBLE -> json.asDouble();
        default -> throw new IllegalArgumentException();
      };
    } else if (json.isNull()) {
      return null;
    }
    throw new UnsupportedOperationException(json.toString());
  }
}
