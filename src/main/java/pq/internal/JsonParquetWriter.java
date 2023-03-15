/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
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

public class JsonParquetWriter {

  public static Builder builder(OutputFile file) {
    return new Builder(file);
  }

  public static final class Builder extends ParquetWriter.Builder<JsonValue, Builder> {

    private MessageType schema;
    private Map<String, String> metadata;

    Builder(OutputFile path) {
      super(path);
    }

    public Builder withSchema(MessageType schema) {
      this.schema = schema;
      return self();
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
      return new JsonWriteSupport(schema, metadata != null ? metadata : new LinkedHashMap<>());
    }
  }
}
