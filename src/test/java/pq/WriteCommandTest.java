/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import com.eclipsesource.json.Json;

class WriteCommandTest {

  @Test
  void parseInt() {
    var schema = new MessageType("schema", Types.required(INT32).named("id"));
    var input = new WriteCommand.CsvInput(schema);
    
    assertThat(input.parse("1")).isEqualTo(Json.object().add("id", 1));
    assertThat(input.parse("")).isEqualTo(Json.object().add("id", Json.NULL));
    assertThat(input.parse("null")).isEqualTo(Json.object().add("id", Json.NULL));
  }

  @Test
  void parseLong() {
    var schema = new MessageType("schema", Types.required(INT64).named("id"));
    var input = new WriteCommand.CsvInput(schema);
    
    assertThat(input.parse("1")).isEqualTo(Json.object().add("id", 1L));
    assertThat(input.parse("")).isEqualTo(Json.object().add("id", Json.NULL));
    assertThat(input.parse("null")).isEqualTo(Json.object().add("id", Json.NULL));
  }

  @Test
  void parseFloat() {
    var schema = new MessageType("schema", Types.required(FLOAT).named("id"));
    var input = new WriteCommand.CsvInput(schema);
    
    assertThat(input.parse("1")).isEqualTo(Json.object().add("id", 1f));
    assertThat(input.parse("")).isEqualTo(Json.object().add("id", Json.NULL));
    assertThat(input.parse("null")).isEqualTo(Json.object().add("id", Json.NULL));
  }

  @Test
  void parseDouble() {
    var schema = new MessageType("schema", Types.required(DOUBLE).named("id"));
    var input = new WriteCommand.CsvInput(schema);
    
    assertThat(input.parse("1")).isEqualTo(Json.object().add("id", 1d));
    assertThat(input.parse("")).isEqualTo(Json.object().add("id", Json.NULL));
    assertThat(input.parse("null")).isEqualTo(Json.object().add("id", Json.NULL));
  }

  @Test
  void parseBoolean() {
    var schema = new MessageType("schema", Types.required(BOOLEAN).named("id"));
    var input = new WriteCommand.CsvInput(schema);
    
    assertThat(input.parse("true")).isEqualTo(Json.object().add("id", true));
    assertThat(input.parse("false")).isEqualTo(Json.object().add("id", false));
    assertThat(input.parse("")).isEqualTo(Json.object().add("id", Json.NULL));
    assertThat(input.parse("null")).isEqualTo(Json.object().add("id", Json.NULL));
  }

  @Test
  void parseString() {
    var schema = new MessageType("schema", Types.required(BINARY).as(stringType()).named("id"));
    var input = new WriteCommand.CsvInput(schema);
    
    assertThat(input.parse("\"hola\"")).isEqualTo(Json.object().add("id", "hola"));
    assertThat(input.parse("hola")).isEqualTo(Json.object().add("id", "hola"));
    assertThat(input.parse("")).isEqualTo(Json.object().add("id", Json.NULL));
    assertThat(input.parse("null")).isEqualTo(Json.object().add("id", Json.NULL));
  }

  @Test
  void parseUnsupported() {
    var schema = new MessageType("schema", Types.required(INT96).named("id"));
    var input = new WriteCommand.CsvInput(schema);
    
    assertThatThrownBy(() -> input.parse("hola")).isInstanceOf(UnsupportedOperationException.class);
  }
}
