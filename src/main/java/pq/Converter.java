/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.util.Objects.requireNonNull;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonObject.Member;
import com.eclipsesource.json.JsonValue;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

class Converter {

  private final Schema schema;

  public Converter(Schema schema) {
    this.schema = requireNonNull(schema);
  }

  GenericRecord toRecord(JsonValue json) {
    if (json instanceof JsonObject object) {
      var value = new GenericData.Record(schema);
      for (Member member : object) {
        Field field = schema.getField(member.getName());
        value.put(member.getName(), convert(field.schema(), member.getValue()));
      }
      return value;
    }
    throw new IllegalArgumentException("must be a json object");
  }

  static Object convert(Schema schema, JsonValue json) {
    if (schema.isUnion()) {
      // XXX: I'm not sure how to manage union types
      return convert(filterNull(schema), json);
    }
    if (json instanceof JsonObject object && schema.getType() == Type.RECORD) {
      var value = new GenericData.Record(schema);
      for (Member member : object) {
        Field field = schema.getField(member.getName());
        value.put(member.getName(), convert(field.schema(), member.getValue()));
      }
      return value;
    } else if (json instanceof JsonArray jsonArray && schema.getType() == Type.ARRAY) {
      var array = new GenericData.Array<>(jsonArray.size(), schema);
      for (JsonValue value : jsonArray) {
        array.add(convert(schema.getElementType(), value));
      }
      return array;
    } else if (json.isNull()) {
      return null;
    }
    return switch (schema.getType()) {
      case STRING -> json.asString();
      case BOOLEAN -> json.asBoolean();
      case INT -> json.asInt();
      case LONG -> json.asLong();
      case FLOAT -> json.asFloat();
      case DOUBLE -> json.asDouble();
      default -> throw new IllegalArgumentException(schema + ":" + json);
    };
  }

  private static Schema filterNull(Schema schema) {
    return schema.getTypes().stream().filter(s -> s.getType() != Type.NULL).findFirst().orElseThrow();
  }
}
