/*
 * Copyright (c) 2023-2024, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import com.eclipsesource.json.JsonValue;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.schema.MessageType;

public final class JsonParquetWriter {

  private JsonParquetWriter() { }

  public static Builder builder(OutputFile file, MessageType schema) {
    return new Builder(file, schema);
  }

  public static final class Builder extends ParquetWriter.Builder<JsonValue, Builder> {

    private MessageType schema;
    private Map<String, String> metadata = new LinkedHashMap<>();

    Builder(OutputFile path, MessageType schema) {
      super(path);
      this.schema = schema;
    }

    public Builder withExtraMetadata(Map<String, String> metadata) {
      this.metadata = metadata;
      return self();
    }

    @Override
    protected Builder self() {
      return this;
    }

    @Override
    protected WriteSupport<JsonValue> getWriteSupport(Configuration conf) {
      return new JsonWriteSupport(schema, metadata);
    }
  }
}
