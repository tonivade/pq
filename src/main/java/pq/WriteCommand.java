/*
 * Copyright (c) 2023-2024, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.nio.file.Files.readString;
import static java.util.Objects.requireNonNull;
import static pq.App.createJsonWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonValue;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "write", description = "create a parquet file from a jsonl stream and a schema")
final class WriteCommand implements Runnable {

  @Parameters(paramLabel = "FILE", description = "destination parquet file")
  private File file;

  @Option(names = "--schema", description = "file with schema definition", paramLabel = "FILE")
  private File schemaFile;

  @Option(names = "--format", description = "input format, json or csv", defaultValue = "json", paramLabel = "JSON|CSV", converter = FormatConverter.class)
  private Format format;

  @Override
  public void run() {
    try {
      var schema = parseSchema();
      var input = createInput(schema);
      try (var output = createJsonWriter(file, schema)) {
        try (var lines = new BufferedReader(new InputStreamReader(System.in)).lines()) {
          lines.map(input::parse).forEach(value -> write(output, value));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Parser createInput(MessageType schema) {
    return switch (format) {
      case JSON -> new JsonInput();
      case CSV -> new CsvInput(schema);
    };
  }

  interface Parser {
    JsonValue parse(String line);
  }

  static final class JsonInput implements Parser {
    @Override
    public JsonValue parse(String line) {
      return Json.parse(line);
    }
  }

  static final class CsvInput implements Parser {

    private static final String EMPTY = "";

    final MessageType schema;

    CsvInput(MessageType schema) {
      this.schema = requireNonNull(schema);
    }

    @Override
    public JsonValue parse(String line) {
      var value = Json.object();
      var values = line.split(",");
      for (int i = 0; i < values.length; i++) {
        var type = schema.getFields().get(i);
        value.add(type.getName(), convert(values[i], type.asPrimitiveType().getPrimitiveTypeName()));
      }
      return value;
    }

    private JsonValue convert(String value, PrimitiveTypeName primitiveTypeName) {
      if (value.equals(EMPTY)) {
        return Json.NULL;
      }
      return switch (primitiveTypeName) {
        case INT32 -> Json.value(Integer.parseInt(value));
        case INT64 -> Json.value(Long.parseLong(value));
        case FLOAT -> Json.value(Float.parseFloat(value));
        case DOUBLE -> Json.value(Double.parseDouble(value));
        case BOOLEAN -> Json.value(Boolean.parseBoolean(value));
        case BINARY -> Json.value(unquote(value));
        default -> throw new UnsupportedOperationException("type not supported: " + primitiveTypeName);
      };
    }

    private String unquote(String value) {
      if (value.startsWith("\"") && value.endsWith("\"")) {
        return value.substring(1, value.length() - 1);
      }
      return value;
    }
  }

  private MessageType parseSchema() throws IOException {
    return MessageTypeParser.parseMessageType(readString(schemaFile.toPath()));
  }

  private void write(ParquetWriter<JsonValue> output, JsonValue value) {
    try {
      output.write(value);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}