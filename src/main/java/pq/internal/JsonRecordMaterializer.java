/*
 * Copyright (c) 2023-2025, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import com.eclipsesource.json.JsonValue;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

final class JsonRecordMaterializer extends RecordMaterializer<JsonValue> {

  private final JsonGroupConverter root;
  private final JsonObjectHolder value;

  JsonRecordMaterializer(MessageType requestedSchema) {
    this.value = new JsonObjectHolder();
    this.root = new JsonGroupConverter(requestedSchema, value.set());
  }

  @Override
  public JsonValue getCurrentRecord() {
    return value.get();
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
