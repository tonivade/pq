/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import static java.util.Objects.requireNonNull;
import com.eclipsesource.json.JsonValue;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

final class JsonWriteSupport extends WriteSupport<JsonValue> {

  private final MessageType schema;
  private final Map<String, String> metadata;

  private JsonMessageWriter writer;

  public JsonWriteSupport(MessageType schema, Map<String, String> metadata) {
    this.schema = requireNonNull(schema);
    this.metadata = requireNonNull(metadata);
  }

  @Override
  public String getName() {
    return schema.getName();
  }

  @Override
  public WriteContext init(Configuration configuration) {
    return new WriteContext(schema, metadata);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    writer = new JsonMessageWriter(recordConsumer, schema);
  }

  @Override
  public void write(JsonValue value) {
    writer.write(value);
  }

}
