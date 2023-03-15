package pq.internal;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import com.eclipsesource.json.JsonValue;

public final class JsonRecordMaterializer extends RecordMaterializer<JsonValue> {
  
  private final JsonGroupConverter root;
  
  private JsonValue value;

  public JsonRecordMaterializer(MessageType requestedSchema) {
    this.root = new JsonGroupConverter(requestedSchema, v -> this.value = v);
  }

  @Override
  public JsonValue getCurrentRecord() {
    return value;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }
}
