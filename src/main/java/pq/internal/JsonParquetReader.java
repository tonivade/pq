/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import com.eclipsesource.json.JsonValue;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

public final class JsonParquetReader {

  private JsonParquetReader() { }

  public static Builder builder(InputFile file) {
    return new Builder(file);
  }

  public static final class Builder extends ParquetReader.Builder<JsonValue> {

    private MessageType projection;

    public Builder(InputFile file) {
      super(file);
    }

    public Builder withProjection(MessageType projection) {
      this.projection = projection;
      return this;
    }

    @Override
    protected ReadSupport<JsonValue> getReadSupport() {
      return new JsonReadSupport(projection);
    }
  }
}
