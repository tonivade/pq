/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import com.eclipsesource.json.JsonValue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

final class JsonReadSupport extends ReadSupport<JsonValue> {

  private final MessageType projection;

  JsonReadSupport(MessageType projection) {
    this.projection = projection;
  }

  @Override
  public RecordMaterializer<JsonValue> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema, ReadContext readContext) {
    return new JsonRecordMaterializer(readContext.getRequestedSchema());
  }

  @Override
  public ReadContext init(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema) {
    return new ReadContext(projection != null ? projection : fileSchema, new LinkedHashMap<>());
  }
}
