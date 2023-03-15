/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonValue;

import java.util.function.Consumer;

final class JsonArrayHolder {

  private JsonArray array;

  void create() {
    array = new JsonArray();
  }

  void accept(Consumer<JsonValue> consumer) {
    consumer.accept(array);
    array = null;
  }

  Consumer<Integer> addInt() {
    return i -> array.add(i);
  }

  Consumer<Long> addLong() {
    return l -> array.add(l);
  }

  Consumer<Float> addFloat() {
    return f -> array.add(f);
  }

  Consumer<Double> addDouble() {
    return d -> array.add(d);
  }

  Consumer<Boolean> addBoolean() {
    return b -> array.add(b);
  }

  Consumer<String> addString() {
    return s -> array.add(s);
  }

  Consumer<JsonValue> addValue() {
    return v -> array.add(v);
  }
}
