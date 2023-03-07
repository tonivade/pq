///usr/bin/env jbang "$0" "$@" ; exit $?

//DEPS org.apache.parquet:parquet-avro:1.12.3
//DEPS com.jerolba:carpet-filestream:0.0.3
//DEPS org.apache.hadoop:hadoop-common:3.3.4
//DEPS org.apache.hadoop:hadoop-mapreduce-client-core:3.3.4
//DEPS info.picocli:picocli:4.7.1

/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
import static java.util.Objects.requireNonNull;
import com.jerolba.carpet.filestream.FileSystemInputFile;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ScopeType;

@Command(name = "pq", description = "parquet query tool", footer = "Copyright(c) 2023 by @tonivade",
  subcommands = { pq.CountCommand.class, pq.SchemaCommand.class, pq.ReadCommand.class, pq.MetadataCommand.class, HelpCommand.class })
public class pq {

  @Command(name = "count", description = "print total number of rows in parquet file")
  public static class CountCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Override
    public void run() {
      try (var reader = createFileReader(file)) {
        System.out.println(reader.getRecordCount());
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
      try (var reader = createFileReader(file)) {
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

    @Option(names = "--skip", description = "skip number of rows", paramLabel = "ROWS", defaultValue = "0")
    private int skip;

    // TODO
    @Option(names = "--extract", description = "field list to extract from parquet file", paramLabel = "FIELDS")
    private String filter;

    @Option(names = "--index", description = "print row index", defaultValue = "false")
    private boolean index;

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    private final Output output = new Output(System.out);

    @Override
    public void run() {
      if (head > 0) {
        stream(file).skip(skip).limit(head).forEach(this::print);
      } else if (tail > 0) {
        stream(file).skip(count() - tail).forEach(this::print);
      } else if (get > -1) {
        stream(file).skip(skip).skip(get).findFirst().ifPresent(this::print);
      } else {
        stream(file).skip(skip).forEach(this::print);
      }
    }

    private long count() {
      try (var reader = createFileReader(file)) {
        return reader.getRecordCount();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private void print(Tuple tuple) {
      if (index) {
        output.printIndex(tuple.index());
      }
      output.printObject(tuple.value());
      output.printSeparator();
    }
  }

  @Option(names = { "-v", "--verbose" }, description = "enable debug logs", scope = ScopeType.INHERIT)
  void setVerbose(boolean verbose) {
    System.setProperty("root-level", verbose ? "DEBUG" : "ERROR");
  }

  public static void main(String... args) {
    System.exit(new CommandLine(new pq()).execute(args));
  }

  private static Stream<Tuple> stream(File file) {
    return StreamSupport.stream(new ParquetIterable(file).spliterator(), false);
  }

  private static ParquetFileReader createFileReader(File file) throws IOException {
    return new ParquetFileReader(new FileSystemInputFile(file), ParquetReadOptions.builder().build());
  }
}

final class ParquetIterable implements Iterable<Tuple> {

  private final File file;

  public ParquetIterable(File file) {
    this.file = requireNonNull(file);
  }

  @Override
  public Iterator<Tuple> iterator() {
    try {
      return new ParquetIterator(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}

final class ParquetIterator implements Iterator<Tuple> {

  private final ParquetReader<GenericRecord> reader;

  private GenericRecord current = null;

  public ParquetIterator(File file) throws IOException {
    this.reader = createParquetReader(file);
  }

  @Override
  public boolean hasNext() {
    return tryAdvance() != null;
  }

  @Override
  public Tuple next() {
    var result = tryAdvance();
    if (result == null) {
      throw new NoSuchElementException();
    }
    current = null;
    return new Tuple(reader.getCurrentRowIndex(), result);
  }

  private GenericRecord tryAdvance() {
    try {
      if (current == null) {
        current = reader.read();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return current;
  }

  private static ParquetReader<GenericRecord> createParquetReader(File file) throws IOException {
    return AvroParquetReader.genericRecordReader(new FileSystemInputFile(file));
  }
}

record Tuple(long index, GenericRecord value) {}

final class Output {

  final PrintStream out;

  Output(PrintStream out) {
    this.out = requireNonNull(out);
  }

  void printIndex(long index) {
    out.println("#" + index);
  }

  void printObject(GenericRecord value) {
    print(null, value, true);
  }

  void printSeparator() {
    out.println();
  }

  private void print(Field field, Object value, boolean last) {
    if (value instanceof GenericArray<?> array) {
      printArray(field, array, last);
    } else if (value instanceof GenericRecord record) {
      printRecord(field, record, last);
    } else if (value instanceof CharSequence string) {
      printString(field, string, last);
    } else {
      printNotString(field, value, last);
    }
  }

  private void printArray(Field field, GenericArray<?> array, boolean last) {
    printField(field);
    out.print("[");
    int i = 0;
    for (var element : array) {
      print(null, element, array.size() == ++i);
    }
    out.print("]");
    printComma(last);
  }

  private void printRecord(Field field, GenericRecord record, boolean last) {
    printField(field);
    out.print("{");
    int i = 0;
    for (var f: record.getSchema().getFields()) {
      print(f, record.get(f.pos()), record.getSchema().getFields().size() == ++i);
    }
    out.print("}");
    printComma(last);
  }

  private void printString(Field field, CharSequence value, boolean last) {
    printField(field);
    out.print("\"" + value + "\"");
    printComma(last);
  }

  private void printNotString(Field field, Object value, boolean last) {
    printField(field);
    out.print(value);
    printComma(last);
  }

  private void printField(Field field) {
    if (field != null) {
      out.print("\"" + field.name() + "\":");
    }
  }

  private void printComma(boolean last) {
    if (!last) {
      out.print(",");
    }
  }
}
