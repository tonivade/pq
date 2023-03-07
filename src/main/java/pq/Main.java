/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import com.jerolba.carpet.filestream.FileSystemInputFile;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.schema.MessageType;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ScopeType;

@Command(name = "pq", description = "parquet query tool", footer = "Copyright(c) 2023 by @tonivade",
  subcommands = { Main.CountCommand.class, Main.SchemaCommand.class, Main.ReadCommand.class, Main.MetadataCommand.class, HelpCommand.class })
public class Main {

  @Command(name = "count", description = "print total number of rows in parquet file")
  public static class CountCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    // FIXME: doesn't work count with filter
    @Option(names = "--filter", description = "predicate to apply to the rows", paramLabel = "PREDICATE")
    private String filter;

    private final FilterParser parser = new FilterParser();

    @Override
    public void run() {
      FilterPredicate predicate = parser.parse(filter);
      try (var reader = createFileReader(file, predicate)) {
        System.out.println(reader.getFilteredRecordCount());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Command(name = "schema", description = "print schema of parquet file")
  public static class SchemaCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Override
    public void run() {
      try (var reader = createFileReader(file, null)) {
        var schema = reader.getFileMetaData().getSchema();
        System.out.print(schema);
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
      try (var reader = createFileReader(file, null)) {
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

    @Option(names = "--skip", description = "skip number of rows", paramLabel = "ROWS", defaultValue = "0")
    private int skip;

    @Option(names = "--filter", description = "predicate to apply to the rows", paramLabel = "PREDICATE")
    private String filter;

    @Option(names = "--select", description = "list of columns to select", paramLabel = "COLUMNS", split = ",")
    private String[] select;

    @Option(names = "--index", description = "print row index", defaultValue = "false")
    private boolean index;

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    private final Printer printer = new Printer(System.out);

    private final FilterParser parser = new FilterParser();

    @Override
    public void run() {
      FilterPredicate predicate = parser.parse(filter);
      long size = size(predicate);
      try (var reader = createParquetReader(file, predicate, schema())) {
        if (head > 0) {
          stream(size, reader).skip(skip).limit(head).forEach(this::print);
        } else if (tail > 0) {
          stream(size, reader).skip(size - tail).forEach(this::print);
        } else if (get > -1) {
          stream(size, reader).skip(skip).skip(get).findFirst().ifPresent(this::print);
        } else {
          stream(size, reader).skip(skip).forEach(this::print);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private Schema schema() {
      try (var reader = createFileReader(file, null)) {
        MessageType schema = reader.getFileMetaData().getSchema();
        if (select != null) {
          Set<String> fields = Set.of(select);
          schema = new MessageType(schema.getName(), schema.getFields().stream().filter(f -> fields.contains(f.getName())).toList());
        }
        return new AvroSchemaConverter().convert(schema);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private long size(FilterPredicate predicate) {
      try (var reader = createFileReader(file, predicate)) {
        return reader.getFilteredRecordCount();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private void print(Tuple tuple) {
      if (index) {
        printer.printIndex(tuple.index());
      }
      printer.printObject(tuple.value());
      printer.printSeparator();
    }
  }

  @Option(names = { "-v", "--verbose" }, description = "enable debug logs", scope = ScopeType.INHERIT)
  void setVerbose(boolean verbose) {
    System.setProperty("root-level", verbose ? "DEBUG" : "ERROR");
  }

  public static void main(String... args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }

  private static Stream<Tuple> stream(long size, ParquetReader<GenericRecord> reader) {
    var spliterator = Spliterators.spliterator(new ParquetIterator(reader), size,
      Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
    return StreamSupport.stream(spliterator, false);
  }

  private static ParquetFileReader createFileReader(File file, FilterPredicate filter) throws IOException {
    if (filter != null) {
      return new ParquetFileReader(new FileSystemInputFile(file),
        ParquetReadOptions.builder().withRecordFilter(FilterCompat.get(filter)).build());
    }
    return new ParquetFileReader(new FileSystemInputFile(file), ParquetReadOptions.builder().build());
  }

  private static ParquetReader<GenericRecord> createParquetReader(File file, FilterPredicate filter, Schema schema) throws IOException {
    var config = new Configuration();
    AvroReadSupport.setAvroReadSchema(config, schema);
    AvroReadSupport.setRequestedProjection(config, schema);
    if (filter != null) {
      return AvroParquetReader.<GenericRecord>builder(new FileSystemInputFile(file))
        .withDataModel(GenericData.get())
        .withFilter(FilterCompat.get(filter))
        .withConf(config)
        .build();
    }
      return AvroParquetReader.<GenericRecord>builder(new FileSystemInputFile(file))
        .withDataModel(GenericData.get())
        .withConf(config)
        .build();
  }
}
