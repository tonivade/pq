/*
 * Copyright (c) 2023-2026, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq;

import com.eclipsesource.json.JsonValue;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
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
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.jspecify.annotations.Nullable;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;
import picocli.CommandLine.ScopeType;
import pq.internal.JsonParquetReader;
import pq.internal.JsonParquetWriter;

@Command(name = "pq", description = "parquet query tool", footer = "Copyright(c) 2023-2025 by @tonivade@mastodon.social",
  subcommands = {
      CountCommand.class, SchemaCommand.class, ReadCommand.class,
      MetadataCommand.class, WriteCommand.class, HelpCommand.class })
public final class App {

  @Option(names = { "-v", "--verbose" }, description = "enable debug logs", scope = ScopeType.INHERIT, defaultValue = "false")
  void setVerbose(boolean verbose) {
    var root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    root.setLevel(verbose ? Level.INFO : Level.ERROR);
  }

  public static void main(String... args) {
    System.exit(execute(args));
  }

  static int execute(String... args) {
    return new CommandLine(new App()).execute(args);
  }

  static MessageType schema(File file) {
    try (var reader = createFileReader(file, FilterCompat.NOOP)) {
      return reader.getFileMetaData().getSchema();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static Filter parseFilter(@Nullable String filter, MessageType schema) {
    if (filter == null) {
      return FilterCompat.NOOP;
    }
    var predicate = new FilterParser().parse(filter).apply(schema).convert();
    return FilterCompat.get(predicate);
  }

  static Optional<MessageType> createProjection(MessageType schema, @Nullable String filter) {
    String[] select = new FilterParser().parse(filter).columns().toArray(String[]::new);
    return createProjection(schema, select);
  }

  static Optional<MessageType> createProjection(MessageType schema, @Nullable String[] select) {
    if (select != null && select.length > 0) {
      Set<String> fields = Set.of(select);
      return Optional.of(new MessageType(
          schema.getName(), schema.getFields().stream().filter(f -> fields.contains(f.getName())).toList()));
    }
    return Optional.empty();
  }

  static Stream<Tuple> stream(ParquetReader<JsonValue> reader) {
    var spliterator = Spliterators.spliteratorUnknownSize(
        new ParquetIterator(reader), Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
    return StreamSupport.stream(spliterator, false);
  }

  static ParquetFileReader createFileReader(File file, Filter filter) throws IOException {
    return new ParquetFileReader(
        new ParquetInputFile(file), ParquetReadOptions.builder().withRecordFilter(filter).build());
  }

  static ParquetWriter<JsonValue> createJsonWriter(File file, MessageType schema) throws IOException {
    return JsonParquetWriter.builder(new ParquetOutputFile(file), schema)
        .withWriteMode(Mode.OVERWRITE)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .build();
  }

  static ParquetReader<JsonValue> createJsonReader(File file, Filter filter, @Nullable MessageType projection) throws IOException {
    return JsonParquetReader.builder(new ParquetInputFile(file))
        .withProjection(projection)
        .withFilter(filter)
        .build();
  }
}
