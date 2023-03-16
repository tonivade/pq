/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.nio.file.Files.readString;

import com.eclipsesource.json.Json;
import com.eclipsesource.json.JsonValue;
import com.jerolba.carpet.filestream.FileSystemInputFile;
import com.jerolba.carpet.filestream.FileSystemOutputFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ScopeType;
import pq.internal.JsonParquetReader;
import pq.internal.JsonParquetWriter;

@Command(name = "pq", description = "parquet query tool", footer = "Copyright(c) 2023 by @tonivade",
  subcommands = { App.CountCommand.class, App.SchemaCommand.class, App.ReadCommand.class, App.MetadataCommand.class, App.WriteCommand.class, HelpCommand.class })
public class App {

  @Command(name = "count", description = "print total number of rows in parquet file")
  public static class CountCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Option(names = "--filter",
        description = "predicate to apply to the rows: *warning* with this option a full scan of the file will be made",
        paramLabel = "PREDICATE")
    private String filter;

    private final FilterParser parser = new FilterParser();

    @Override
    public void run() {
      MessageType schema = schema(file);
      FilterPredicate predicate = parser.parse(filter).apply(schema).convert();
      long count = size(file, predicate);
      System.out.println(count);
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
      try (var reader = createFileReader(file)) {
        MessageType schema = reader.getFileMetaData().getSchema();
        var projection = projection(schema, select).orElse(schema);
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

    @Override
    public void run() {
      try (var reader = createFileReader(file)) {
        reader.getFileMetaData().getKeyValueMetaData()
          .forEach((k, v) -> System.out.println("\"" + k + "\":" + v));
        System.out.println("\"createdBy\":" + reader.getFileMetaData().getCreatedBy());
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

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    private final FilterParser parser = new FilterParser();

    @Override
    public void run() {
      MessageType schema = schema(file);
      FilterPredicate predicate = parser.parse(filter).apply(schema).convert();
      try (var reader = createJsonReader(file, predicate, projection(schema, select).orElse(null))) {
        if (head > 0) {
          stream(reader).skip(skip).limit(head).forEach(this::print);
        } else if (tail > 0) {
          // XXX: this needs read twice the file if filter is not null
          stream(reader).skip(size(file, predicate) - tail).forEach(this::print);
        } else if (get > -1) {
          stream(reader).skip(skip).skip(get).findFirst().ifPresent(this::print);
        } else {
          stream(reader).skip(skip).forEach(this::print);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private void print(Tuple tuple) {
      if (index) {
        System.out.println("#" + tuple.index());
      }
      System.out.println(tuple.value());
    }
  }

  @Command(name = "write", description = "create a parquet file from a jsonl stream and a avro schema")
  public static class WriteCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "destination parquet file")
    private File file;

    @Option(names = "--schema", description = "file with schema", paramLabel = "FILE")
    private File schemaFile;

    @Option(names = "--format", description = "schema format, parquet or avro", paramLabel = "FORMAT", defaultValue = "parquet")
    private String format;

    @Override
    public void run() {
      try {
        var schema = parseSchema();
        try (var output = createJsonWriter(file, schema)) {
          try (var lines = new BufferedReader(new InputStreamReader(System.in)).lines()) {
            lines.map(Json::parse).forEach(value -> write(output, value));
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
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

  private static long size(File file, FilterPredicate predicate) {
    if (predicate != null) {
      try (var reader = createJsonReader(file, predicate, null)) {
        return stream(reader).count();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    try (var reader = createFileReader(file)) {
      return reader.getRecordCount();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static MessageType schema(File file) {
    try (var reader = createFileReader(file)) {
      return reader.getFileMetaData().getSchema();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Optional<MessageType> projection(MessageType schema, String[] select) {
    if (select != null) {
      Set<String> fields = Set.of(select);
      return Optional.of(new MessageType(
          schema.getName(), schema.getFields().stream().filter(f -> fields.contains(f.getName())).toList()));
    }
    return Optional.empty();
  }

  private static Stream<Tuple> stream(ParquetReader<JsonValue> reader) {
    var spliterator = Spliterators.spliteratorUnknownSize(new ParquetIterator(reader), Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
    return StreamSupport.stream(spliterator, false);
  }

  private static ParquetFileReader createFileReader(File file) throws IOException {
    return new ParquetFileReader(new FileSystemInputFile(file), ParquetReadOptions.builder().build());
  }

  private static ParquetWriter<JsonValue> createJsonWriter(File file, MessageType schema) throws IOException {
    return JsonParquetWriter.builder(new FileSystemOutputFile(file))
        .withWriteMode(Mode.OVERWRITE)
        .withSchema(schema)
        .build();
  }

  private static ParquetReader<JsonValue> createJsonReader(File file, FilterPredicate filter, MessageType projection) throws IOException {
    if (filter != null) {
      return JsonParquetReader.builder(new FileSystemInputFile(file))
          .withProjection(projection)
          .withFilter(FilterCompat.get(filter))
          .build();
    }
    return JsonParquetReader.builder(new FileSystemInputFile(file))
        .withProjection(projection)
        .build();
  }
}
