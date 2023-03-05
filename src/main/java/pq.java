///usr/bin/env jbang "$0" "$@" ; exit $?

//DEPS org.apache.parquet:parquet-avro:1.12.3
//DEPS org.apache.hadoop:hadoop-client:3.3.2
//DEPS info.picocli:picocli:4.7.1

/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
import static java.util.Objects.requireNonNull;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
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
        stream(file).skip(skip).skip(get - 1).findFirst().ifPresent(this::printLine);
      } else {
        stream(file).skip(skip).forEach(this::printLine);
      }
    }

    private void printLine(Tuple tuple) {
      if (counter) {
        System.out.println(tuple.counter());
      }
      System.out.print(toJson(tuple.value()));
    }

    private String toJson(GenericRecord value) {
      var out = new ByteArrayOutputStream();
      print(new PrintStream(out), "", null, value, true);
      return out.toString(StandardCharsets.UTF_8);
    }
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new pq()).execute(args));
  }

  private static void print(PrintStream out, String ident, Field field, Object value, boolean last) {
    if (value instanceof GenericArray<?> array) {
      printArray(out, ident, field, array, last);
    } else if (value instanceof GenericRecord record) {
      printRecord(out, ident, field, record, last);
    } else if (value instanceof CharSequence) {
      printString(out, ident, field, value, last);
    } else {
      printNotString(out, ident, field, value, last);
    }
  }

  private static void printArray(PrintStream out, String ident, Field field, GenericArray<?> array, boolean last) {
    out.println(ident + "\"" + field.name() + "\": [");
    int i = 0;
    for (var element : array) {
      print(out, ident + "    ", null, element, array.size() == ++i);
    }
    if (last) {
      out.println(ident + "]");
    } else {
      out.println(ident + "],");
    }
  }

  private static void printRecord(PrintStream out, String ident, Field field, GenericRecord record, boolean last) {
    if (field != null) {
      out.println(ident + "\"" + field.name() + "\": {");
    } else {
      out.println(ident + "{");
    }
    int i = 0;
    for (var f: record.getSchema().getFields()) {
      print(out, ident + "    ", f, record.get(f.pos()), record.getSchema().getFields().size() == ++i);
    }
    if (last) {
      out.println(ident + "}");
    } else {
      out.println(ident + "},");
    }
  }

  private static void printString(PrintStream out, String ident, Field field, Object value, boolean last) {
    if (last) {
      out.println(ident + "\"" + field.name() + "\": \"" + value + "\"");
    } else {
      out.println(ident + "\"" + field.name() + "\": \"" + value + "\",");
    }
  }

  private static void printNotString(PrintStream out, String ident, Field field, Object value, boolean last) {
    if (last) {
      out.println(ident + "\"" + field.name() + "\": " + value);
    } else {
      out.println(ident + "\"" + field.name() + "\": " + value + ",");
    }
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
