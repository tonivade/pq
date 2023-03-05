///usr/bin/env jbang "$0" "$@" ; exit $?

//DEPS org.apache.parquet:parquet-avro:1.12.3
//DEPS org.apache.hadoop:hadoop-client:3.3.2
//DEPS info.picocli:picocli:4.7.1

/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
import static java.util.Objects.requireNonNull;
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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "pq", description = "parquet query tool", footer = "Copyright(c) 2023 by @tonivade",
  subcommands = { pq.CountCommand.class, pq.SchemaCommand.class, pq.ParseCommand.class, HelpCommand.class })
public class pq {

  @Command(name = "count", description = "print total number of rows in parquet file")
  public static class CountCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Override
    public void run() {
      long count = stream(file).count();
      System.out.println(count);
    }
  }

  @Command(name = "schema", description = "print avro schema of parquet file")
  public static class SchemaCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Override
    public void run() {
      var schema = stream(file).findFirst().orElseThrow().value().getSchema();
      System.out.println(schema);
    }
  }

  @Command(name = "parse", description = "print content of parquet file in json format")
  public static class ParseCommand implements Runnable {

    @Option(names = "--limit", description = "limit number of elements", paramLabel = "LIMIT")
    private int limit;

    @Option(names = "--get", description = "print just the element number X", paramLabel = "GET")
    private int get;

    @Option(names = "--skip", description = "skip number of element", paramLabel = "SKIP")
    private int skip;

    // TODO
    @Option(names = "--extract", description = "field list to extract from parquet file", paramLabel = "FIELDS")
    private String filter;

    @Option(names = "--counter", description = "print counter")
    private boolean counter;

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Override
    public void run() {
      if (limit > 0) {
        stream(file).skip(skip).limit(limit).forEach(this::printLine);
      } else if (get > 0) {
        stream(file).skip(skip).skip(get - 1l).findFirst().ifPresent(this::printLine);
      } else {
        stream(file).skip(skip).forEach(this::printLine);
      }
    }

    private void printLine(Tuple tuple) {
      if (counter) {
        System.out.println("#" + tuple.counter());
      }
      Json.write(System.out, tuple.value());
      System.out.println();
    }
  }

  public static void main(String... args) {
    System.exit(new CommandLine(new pq()).execute(args));
  }

  private static Stream<Tuple> stream(File file) {
    return StreamSupport.stream(new ParquetIterable(file).spliterator(), false);
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

  private int counter;
  private GenericRecord current = null;

  public ParquetIterator(File file) throws IOException {
    this.reader = createParquetReader(file.toPath());
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
    counter++;
    current = null;
    return new Tuple(counter, result);
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

  private static ParquetReader<GenericRecord> createParquetReader(java.nio.file.Path tmpPath) throws IOException {
    var inputFile = HadoopInputFile.fromPath(new Path(tmpPath.toUri()), new Configuration());
    return AvroParquetReader.<GenericRecord>builder(inputFile)
      .withDataModel(GenericData.get())
      .build();
  }
}

record Tuple(int counter, GenericRecord value) {}

final class Json {

  static void write(PrintStream out, Object value) {
    write(out, null, value, true);
  }

  static void write(PrintStream out, Field field, Object value, boolean last) {
    if (value instanceof GenericArray<?> array) {
      writeArray(out, field, array, last);
    } else if (value instanceof GenericRecord record) {
      writeRecord(out, field, record, last);
    } else if (value instanceof CharSequence string) {
      writeString(out, field, string, last);
    } else {
      writeNotString(out, field, value, last);
    }
  }

  private static void writeArray(PrintStream out, Field field, GenericArray<?> array, boolean last) {
    out.print("\"" + field.name() + "\":[");
    int i = 0;
    for (var element : array) {
      write(out, null, element, array.size() == ++i);
    }
    out.print("]");
    writeComma(out, last);
  }

  private static void writeRecord(PrintStream out, Field field, GenericRecord record, boolean last) {
    if (field != null) {
      out.print("\"" + field.name() + "\":{");
    } else {
      out.print("{");
    }
    int i = 0;
    for (var f: record.getSchema().getFields()) {
      write(out, f, record.get(f.pos()), record.getSchema().getFields().size() == ++i);
    }
    out.print("}");
    writeComma(out, last);
  }

  private static void writeString(PrintStream out, Field field, CharSequence value, boolean last) {
    out.print("\"" + field.name() + "\":\"" + value + "\"");
    writeComma(out, last);
  }

  private static void writeNotString(PrintStream out, Field field, Object value, boolean last) {
    out.print("\"" + field.name() + "\":" + value);
    writeComma(out, last);
  }

  private static void writeComma(PrintStream out, boolean last) {
    if (!last) {
      out.print(",");
    }
  }
}
