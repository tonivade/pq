package pq.internal;

import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

import com.eclipsesource.json.JsonValue;

public final class JsonParquetReader {
  
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
