///usr/bin/env jbang "$0" "$@" ; exit $?

//DEPS org.apache.parquet:parquet-avro:1.12.3
//DEPS org.apache.hadoop:hadoop-client:3.3.2
//DEPS info.picocli:picocli:4.7.1

/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

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

@Command(name = "pq", description = "parquet query tool", 
  subcommands = { pq.CountCommand.class, pq.SchemaCommand.class, pq.ParseCommand.class, HelpCommand.class })
public class pq {

  @Command(name = "count", description = "print total number of rows in parquet file")
  public static class CountCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Override
    public void run() {
      int i = 0;
      try (var reader = createParquetReader(file.toPath())) {
        var nextRecord = reader.read();
        while (nextRecord != null) {
          i++;
          nextRecord = reader.read();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      System.out.println(i);
    }
  }

  @Command(name = "schema", description = "print avro schema of parquet file")
  public static class SchemaCommand implements Runnable {

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Override
    public void run() {
      try (var reader = createParquetReader(file.toPath())) {
        var schema = reader.read().getSchema();
        System.out.println(schema);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Command(name = "parse", description = "print content of parquet file in json format")
  public static class ParseCommand implements Runnable {

    @Option(names = "--limit", description = "limit number of elements", paramLabel = "LIMIT")
    private int limit;

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    @Override
    public void run() {
      try (var reader = createParquetReader(file.toPath())) {
        int i = 0;
        var nextRecord = reader.read();
        while (nextRecord != null) {
          if (limit != 0 && ++i > limit) {
            break;
          }
          print("", null, nextRecord, true);
          nextRecord = reader.read();
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  public static void main(String[] args) {
    System.exit(new CommandLine(new pq()).execute(args));
  }

  private static void print(String ident, Field field, Object value, boolean last) {
    if (value instanceof GenericArray<?> array) {
      printArray(ident, field, array, last);
    } else if (value instanceof GenericRecord record) {
      printRecord(ident, field, record, last);
    } else if (value instanceof CharSequence) {
      printString(ident, field, value, last);
    } else {
      printNotString(ident, field, value, last);
    }
  }

  private static void printArray(String ident, Field field, GenericArray<?> array, boolean last) {
    System.out.println(ident + "\"" + field.name() + "\": [");
    int i = 0;
    for (var element : array) {
      print(ident + "    ", null, element, array.size() == ++i);
    }
    if (last) {
      System.out.println(ident + "]");
    } else {
      System.out.println(ident + "],");
    }
  }

  private static void printRecord(String ident, Field field, GenericRecord record, boolean last) {
    if (field != null) {
      System.out.println(ident + "\"" + field.name() + "\": {");
    } else {
      System.out.println(ident + "{");
    }
    int i = 0;
    for (var f: record.getSchema().getFields()) {
      print(ident + "    ", f, record.get(f.pos()), record.getSchema().getFields().size() == ++i);
    }
    if (last) {
      System.out.println(ident + "}");
    } else {
      System.out.println(ident + "},");
    }
  }

  private static void printString(String ident, Field field, Object value, boolean last) {
    if (last) {
      System.out.println(ident + "\"" + field.name() + "\": \"" + value + "\"");
    } else {
      System.out.println(ident + "\"" + field.name() + "\": \"" + value + "\",");
    }
  }

  private static void printNotString(String ident, Field field, Object value, boolean last) {
    if (last) {
      System.out.println(ident + "\"" + field.name() + "\": " + value);
    } else {
      System.out.println(ident + "\"" + field.name() + "\": " + value + ",");
    }
  }

  private static ParquetReader<GenericRecord> createParquetReader(java.nio.file.Path tmpPath) throws IOException {
    var inputFile = HadoopInputFile.fromPath(new Path(tmpPath.toUri()), new Configuration());
    return AvroParquetReader.<GenericRecord>builder(inputFile)
      .withDataModel(GenericData.get())
      .build();
  }
}
