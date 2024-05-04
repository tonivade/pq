/*
 * Copyright (c) 2023-2024, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import static java.util.Objects.requireNonNull;

import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;

final class JsonValueWriter {

  private final RecordConsumer consumer;
  private final GroupType schema;

  JsonValueWriter(RecordConsumer recordConsumer, GroupType schema) {
    this.consumer = requireNonNull(recordConsumer);
    this.schema = requireNonNull(schema);
  }

  void write(JsonValue value) {
    if (value.isNull()) {
      // nothing to do
    } else if (value.isObject()) {
      writeObject(value.asObject());
    } else if (value.isArray()) {
      // TODO
      throw new UnsupportedOperationException();
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private void writeObject(JsonObject value) {
    for (var fieldType : schema.getFields()) {
      if (fieldType.isPrimitive()) {
        String fieldName = fieldType.getName();
        JsonValue fieldValue = value.get(fieldName);
        if (fieldValue.isNull()) {
          continue;
        }
        switch (fieldType.asPrimitiveType().getPrimitiveTypeName()) {
          case INT32 -> writeInt(fieldName, fieldValue.asInt());
          case INT64 -> writeLong(fieldName, fieldValue.asLong());
          case FLOAT -> writeFloat(fieldName, fieldValue.asFloat());
          case DOUBLE -> writeDouble(fieldName, fieldValue.asDouble());
          case BOOLEAN -> writeBoolean(fieldName, fieldValue.asBoolean());
          case BINARY -> writeString(fieldName, fieldValue.asString());
          default -> throw new UnsupportedOperationException("not supported type: " + fieldType);
        }
      } else {
        // TODO
        throw new UnsupportedOperationException();
      }
    }
  }

  private void writeInt(String name, int value) {
    consumer.startField(name, schema.getFieldIndex(name));
    consumer.addInteger(value);
    consumer.endField(name, schema.getFieldIndex(name));
  }

  private void writeLong(String name, long value) {
    consumer.startField(name, schema.getFieldIndex(name));
    consumer.addLong(value);
    consumer.endField(name, schema.getFieldIndex(name));
  }

  private void writeFloat(String name, float value) {
    consumer.startField(name, schema.getFieldIndex(name));
    consumer.addFloat(value);
    consumer.endField(name, schema.getFieldIndex(name));
  }

  private void writeDouble(String name, double value) {
    consumer.startField(name, schema.getFieldIndex(name));
    consumer.addDouble(value);
    consumer.endField(name, schema.getFieldIndex(name));
  }

  private void writeBoolean(String name, boolean value) {
    consumer.startField(name, schema.getFieldIndex(name));
    consumer.addBoolean(value);
    consumer.endField(name, schema.getFieldIndex(name));
  }

  private void writeString(String name, String value) {
    consumer.startField(name, schema.getFieldIndex(name));
    consumer.addBinary(Binary.fromString(value));
    consumer.endField(name, schema.getFieldIndex(name));
  }
}
