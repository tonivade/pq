/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import static java.util.Objects.requireNonNull;
import com.eclipsesource.json.JsonValue;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

public class JsonMessageWriter {

  private final RecordConsumer consumer;
  private final JsonValueWriter writer;

  JsonMessageWriter(RecordConsumer recordConsumer, MessageType schema) {
    this.consumer = requireNonNull(recordConsumer);
    this.writer = new JsonValueWriter(recordConsumer, schema);
  }

  void write(JsonValue value) {
    consumer.startMessage();
    writer.write(value);
    consumer.endMessage();
  }
}
