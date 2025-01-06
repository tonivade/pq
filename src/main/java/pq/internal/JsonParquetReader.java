/*
 * Copyright (c) 2023-2025, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import com.eclipsesource.json.JsonValue;

import javax.annotation.Nullable;

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

    @Nullable
    private MessageType projection;

    public Builder(InputFile file) {
      super(file);
    }

    public Builder withProjection(@Nullable MessageType projection) {
      this.projection = projection;
      return this;
    }

    @Override
    protected ReadSupport<JsonValue> getReadSupport() {
      return new JsonReadSupport(projection);
    }
  }
}
