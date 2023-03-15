/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

import java.util.function.Consumer;

import org.apache.parquet.schema.GroupType;

final class JsonObjectHolder {

  private JsonObject value;

  void create(GroupType schema) {
    this.value = new JsonObject();
    initFieldsWithNull(schema);
  }

  JsonObject get() {
    return value;
  }

  void accept(Consumer<JsonValue> consumer) {
    consumer.accept(value);
    value = null;
  }

  Consumer<Integer> setInt(String field) {
    return i -> value.set(field, i);
  }

  Consumer<Long> setLong(String field) {
    return l -> value.set(field, l);
  }

  Consumer<Float> setFloat(String field) {
    return f -> value.set(field, f);
  }

  Consumer<Double> setDouble(String field) {
    return d -> value.set(field, d);
  }

  Consumer<Boolean> setBoolean(String field) {
    return b -> value.set(field, b);
  }

  Consumer<String> setString(String field) {
    return s -> value.set(field, s);
  }

  Consumer<JsonValue> setValue(String field) {
    return v -> value.set(field, v);
  }

  Consumer<Integer> addInt(String field) {
    return i -> asArray(field).add(i);
  }

  Consumer<Long> addLong(String field) {
    return l -> asArray(field).add(l);
  }

  Consumer<Float> addFloat(String field) {
    return f -> asArray(field).add(f);
  }

  Consumer<Double> addDouble(String field) {
    return d -> asArray(field).add(d);
  }

  Consumer<Boolean> addBoolean(String field) {
    return b -> asArray(field).add(b);
  }

  Consumer<String> addString(String field) {
    return s -> asArray(field).add(s);
  }

  Consumer<JsonValue> addValue(String field) {
    return v -> asArray(field).add(v);
  }

  Consumer<JsonValue> set() {
    return v -> this.value = v.asObject();
  }

  private void initFieldsWithNull(GroupType schema) {
    for (var fieldType : schema.getFields()) {
      value.add(fieldType.getName(), Json.NULL);
    }
  }

  private JsonArray asArray(String field) {
    if (value.get(field).isNull()) {
      value.set(field, new JsonArray());
    }
    return value.get(field).asArray();
  }
}
