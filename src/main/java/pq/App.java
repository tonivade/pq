/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.nio.file.Files.readString;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonValue;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ScopeType;
import pq.internal.JsonParquetReader;
import pq.internal.JsonParquetWriter;

@Command(name = "pq", description = "parquet query tool", footer = "Copyright(c) 2023 by @tonivade",
  subcommands = { App.CountCommand.class, App.SchemaCommand.class, App.ReadCommand.class, App.MetadataCommand.class, App.WriteCommand.class, HelpCommand.class })
public class App {

  enum Format {
    JSON, CSV;
  }

  static final class FormatConverter implements ITypeConverter<Format> {

    @Override
    public Format convert(String value) {
      return Format.valueOf(value.toUpperCase());
    }
  }

  @Command(name = "count", description = "print total number of rows in parquet file")
  public static class CountCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Option(names = "--filter",
        description = "predicate to apply to the rows",
        paramLabel = "PREDICATE")
    private String filter;

    @Override
    public void run() {
      MessageType schema = schema(file);
      Filter parseFilter = parseFilter(filter, schema);
      MessageType projection = createProjection(schema, filter).orElseGet(() -> justOneColumn(schema));
      try (var reader = createJsonReader(file, parseFilter, projection)) {
        var count = stream(reader).count();
        System.out.println(count);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private MessageType justOneColumn(MessageType schema) {
      return new MessageType(schema.getName(), schema.getFields().get(0));
    }
  }

  @Command(name = "schema", description = "print schema of parquet file")
  public static class SchemaCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Option(names = "--select", description = "list of columns to select", paramLabel = "COLUMN", split = ",")
    private String[] select;

    @Override
    public void run() {
      try (var reader = createFileReader(file, FilterCompat.NOOP)) {
        MessageType schema = reader.getFileMetaData().getSchema();
        var projection = createProjection(schema, select).orElse(schema);
        System.out.print(projection);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Command(name = "metadata", description = "print metadata of parquet file")
  public static class MetadataCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Option(names = "--show-blocks", description = "show block metadata info", defaultValue = "false")
    private boolean showBlocks;

    @Override
    public void run() {
      try (var reader = createFileReader(file, FilterCompat.NOOP)) {
        reader.getFileMetaData().getKeyValueMetaData()
          .forEach((k, v) -> System.out.println("\"" + k + "\":" + v));
        System.out.println("\"createdBy\":" + reader.getFileMetaData().getCreatedBy());
        System.out.println("\"count\":" + reader.getRecordCount());

        if (showBlocks) {
          for (var block : reader.getFooter().getBlocks()) {
            System.out.println("\"block\":" + block.getOrdinal() + ", \"rowCount\":" + block.getRowCount());
            for (var column : block.getColumns()) {
              System.out.println(
                  "\"column\":" + column.getPath() + "," +
                  "\"type\":\"" + column.getPrimitiveType() + "\"," +
                  "\"index\":" + (column.getColumnIndexReference() != null) + "," +
                  "\"dictionary\":" + column.hasDictionaryPage() + "," +
                  "\"encrypted\":" + column.isEncrypted() + "," +
                  "\"stats\":[" + column.getStatistics() + "]"
              );
            }
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Command(name = "read", description = "print content of parquet file in json format")
  public static class ReadCommand implements Runnable {

    @Option(names = "--head", description = "get the first N number of rows", paramLabel = "ROWS", defaultValue = "0")
    private int head;

    @Option(names = "--tail", description = "get the last N number of rows", paramLabel = "ROWS", defaultValue = "0")
    private int tail;

    @Option(names = "--get", description = "print just the row with given index", paramLabel = "ROW", defaultValue = "-1")
    private int get;

    @Option(names = "--skip", description = "skip a number N of rows", paramLabel = "ROWS", defaultValue = "0")
    private int skip;

    @Option(names = "--filter", description = "predicate to apply to the rows", paramLabel = "PREDICATE")
    private String filter;

    @Option(names = "--select", description = "list of columns to select", paramLabel = "COLUMN", split = ",")
    private String[] select;

    @Option(names = "--index", description = "print row index", defaultValue = "false")
    private boolean index;

    @Option(names = "--format", description = "output format, json or csv", defaultValue = "json", converter = FormatConverter.class)
    private Format format;

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Override
    public void run() {
      MessageType schema = schema(file);
      Optional<MessageType> projection = createProjection(schema, select);
      Output output = createOutput(projection.orElse(schema));
      try (var reader = createJsonReader(file, parseFilter(filter, schema), projection.orElse(null))) {
        if (head > 0) {
          stream(reader).skip(skip).limit(head).forEach(output::printRow);
        } else if (tail > 0) {
          ArrayDeque<Tuple> deque = new ArrayDeque<>(tail);
          stream(reader).skip(skip).forEach(i -> {
            if (deque.size() == tail) {
              deque.removeFirst();
            }
            deque.addLast(i);
          });
          deque.forEach(output::printRow);
        } else if (get > -1) {
          stream(reader).skip(skip).skip(get).findFirst().ifPresent(output::printRow);
        } else {
          stream(reader).skip(skip).forEach(output::printRow);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private Output createOutput(MessageType schema) {
      return switch(format) {
        case CSV -> new CsvOutput(schema).printHeader();
        case JSON -> new JsonOutput();
      };
    }

    interface Output {
      void printRow(Tuple tuple);
    }

    final class JsonOutput implements Output {
      @Override
      public void printRow(Tuple tuple) {
        if (index) {
          System.out.println("#" + tuple.index());
        }
        System.out.println(tuple.value());
      }
    }

    final class CsvOutput implements Output {

      private final List<String> columns;

      CsvOutput(MessageType schema) {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < schema.getFieldCount(); i++) {
          names.add(schema.getFieldName(i));
        }
        this.columns = List.copyOf(names);
      }

      @Override
      public void printRow(Tuple tuple) {
        List<String> values = new ArrayList<>();
        for (String column : columns) {
          var value = tuple.value().asObject().get(column);
          values.add(value.toString());
        }
        System.out.println(values.stream().collect(joining(",")));
      }

      CsvOutput printHeader() {
        System.out.println(columns.stream().collect(joining(",")));
        return this;
      }
    }
  }

  @Command(name = "write", description = "create a parquet file from a jsonl stream and a avro schema")
  public static class WriteCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "destination parquet file")
    private File file;

    @Option(names = "--schema", description = "file with schema", paramLabel = "FILE")
    private File schemaFile;

    @Option(names = "--format", description = "input format, json or csv", defaultValue = "json", converter = FormatConverter.class)
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

    private Input createInput(MessageType schema) {
      return switch (format) {
        case JSON -> new JsonInput();
        case CSV -> new CsvInput(schema);
      };
    }

    interface Input {
      JsonValue parse(String line);
    }

    static final class JsonInput implements Input {
      @Override
      public JsonValue parse(String line) {
        return Json.parse(line);
      }
    }

    static final class CsvInput implements Input {

      final MessageType schema;
      
      public CsvInput(MessageType schema) {
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

    void write(ParquetWriter<JsonValue> output, JsonValue value) {
      try {
        output.write(value);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Option(names = { "-v", "--verbose" }, description = "enable debug logs", scope = ScopeType.INHERIT)
  void setVerbose(boolean verbose) {
    System.setProperty("root-level", verbose ? "DEBUG" : "ERROR");
  }

  public static void main(String... args) {
    System.exit(new CommandLine(new App()).execute(args));
  }

  private static MessageType schema(File file) {
    try (var reader = createFileReader(file, FilterCompat.NOOP)) {
      return reader.getFileMetaData().getSchema();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Filter parseFilter(String filter, MessageType schema) {
    if (filter == null) {
      return FilterCompat.NOOP;
    }
    var predicate = new FilterParser().parse(filter).apply(schema).convert();
    return FilterCompat.get(predicate);
  }

  private static Optional<MessageType> createProjection(MessageType schema, String filter) {
    String[] select = new FilterParser().parse(filter).columns().toArray(String[]::new);
    return createProjection(schema, select);
  }

  private static Optional<MessageType> createProjection(MessageType schema, String[] select) {
    if (select != null && select.length > 0) {
      Set<String> fields = Set.of(select);
      return Optional.of(new MessageType(
          schema.getName(), schema.getFields().stream().filter(f -> fields.contains(f.getName())).toList()));
    }
    return Optional.empty();
  }

  private static Stream<Tuple> stream(ParquetReader<JsonValue> reader) {
    var spliterator = Spliterators.spliteratorUnknownSize(
        new ParquetIterator(reader), Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
    return StreamSupport.stream(spliterator, false);
  }

  private static ParquetFileReader createFileReader(File file, Filter filter) throws IOException {
    return new ParquetFileReader(
        new ParquetInputFile(file), ParquetReadOptions.builder().withRecordFilter(filter).build());
  }

  private static ParquetWriter<JsonValue> createJsonWriter(File file, MessageType schema) throws IOException {
    return JsonParquetWriter.builder(new ParquetOutputFile(file))
        .withWriteMode(Mode.OVERWRITE)
        .withSchema(schema)
        .build();
  }

  private static ParquetReader<JsonValue> createJsonReader(File file, Filter filter, MessageType projection) throws IOException {
    return JsonParquetReader.builder(new ParquetInputFile(file))
        .withProjection(projection)
        .withFilter(filter)
        .build();
  }
}
