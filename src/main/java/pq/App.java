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
  subcommands = { App.CountCommand.class, App.SchemaCommand.class, App.ReadCommand.class, App.MetadataCommand.class, HelpCommand.class })
public class App {

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

    @Option(names = "--format", description = "schema format, parquet or avro", paramLabel = "FORMAT", defaultValue = "parquet")
    private String format;

    @Override
    public void run() {
      try (var reader = createFileReader(file, null)) {
        var schema = reader.getFileMetaData().getSchema();
        if (format.equals("parquet")) {
          System.out.print(schema);
        } else if (format.equals("avro")) {
          System.out.println(new AvroSchemaConverter().convert(schema));
        } else {
          new IllegalArgumentException("invalid format: " + format);
        }
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

    private final Printer printer = new Printer(System.out);

    private final FilterParser parser = new FilterParser();

    @Override
    public void run() {
      FilterPredicate predicate = parser.parse(filter);
      try (var reader = createParquetReader(file, predicate, projection())) {
        if (head > 0) {
          stream(size(predicate), reader).skip(skip).limit(head).forEach(this::print);
        } else if (tail > 0) {
          stream(size(predicate), reader).skip(size(predicate) - tail).forEach(this::print);
        } else if (get > -1) {
          stream(size(predicate), reader).skip(skip).skip(get).findFirst().ifPresent(this::print);
        } else {
          stream(size(predicate), reader).skip(skip).forEach(this::print);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private Schema projection() {
      try (var reader = createFileReader(file, null)) {
        MessageType schema = reader.getFileMetaData().getSchema();
        if (select != null) {
          Set<String> fields = Set.of(select);
          var projection = new MessageType(schema.getName(), schema.getFields().stream().filter(f -> fields.contains(f.getName())).toList());
          return new AvroSchemaConverter().convert(projection);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return null;
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
    System.exit(new CommandLine(new App()).execute(args));
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

  private static ParquetReader<GenericRecord> createParquetReader(File file, FilterPredicate filter, Schema projection) throws IOException {
    var config = new Configuration();
    if (projection != null) {
//      AvroReadSupport.setAvroReadSchema(config, schema);
      AvroReadSupport.setRequestedProjection(config, projection);
    }
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
