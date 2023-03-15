package pq.internal;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import com.eclipsesource.json.JsonValue;

public final class JsonReadSupport extends ReadSupport<JsonValue> {
  
  private final MessageType projection;

  public JsonReadSupport(MessageType projection) {
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
