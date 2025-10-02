/*
 * Copyright (c) 2023-2025, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.jspecify.annotations.Nullable;

import com.eclipsesource.json.JsonValue;

final class JsonReadSupport extends ReadSupport<JsonValue> {

  @Nullable
  private final MessageType projection;

  JsonReadSupport(@Nullable MessageType projection) {
    this.projection = projection;
  }

  @Override
  public RecordMaterializer<JsonValue> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData,
      MessageType fileSchema, ReadContext readContext) {
    return new JsonRecordMaterializer(readContext.getRequestedSchema());
  }

  @Override
  public ReadContext init(InitContext context) {
    return new ReadContext(projection != null ? projection : context.getFileSchema(), new LinkedHashMap<>());
  }
}
