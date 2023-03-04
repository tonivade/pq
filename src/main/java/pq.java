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
import java.util.concurrent.Callable;
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
import picocli.CommandLine.ITypeConverter;
import picocli.CommandLine.Parameters;

@Command(name = "pq", description = "parquet query tool")
public class pq implements Callable<Integer> {

  public enum Action { COUNT, SCHEMA, PARSE }

  public static final class ActionConverter implements ITypeConverter<Action> {

    @Override
    public Action convert(String value) {
      return Action.valueOf(value.toUpperCase());
    }

  }

  @Parameters(paramLabel = "ACTION", description = "action to execute: count, schema or parse", index = "0", converter = ActionConverter.class)
  private Action action;

  @Parameters(paramLabel = "FILE", description = "parquet file", index = "1")
  private File file;

  public static void main(String[] args) {
    var exitCode = new CommandLine(new pq()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    switch (action) {
      case COUNT -> count(file);
      case SCHEMA -> schema(file);
      case PARSE -> parse(file);
    }
    return 0;
  }

  private static void parse(File file) {
    try (var reader = createParquetReader(file.toPath())) {
      var nextRecord = reader.read();
      while (nextRecord != null) {
        print("", null, nextRecord);
        nextRecord = reader.read();
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void schema(File file) {
    try (var reader = createParquetReader(file.toPath())) {
      var schema = reader.read().getSchema();
      System.out.println(schema);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void count(File file) {
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

  private static void print(String ident, Field field, Object value) {
    if (value instanceof GenericArray<?> array) {
      System.out.println(ident + field.name() + " [");
      for (var element : array) {
        print(ident + "    ", null, element);
      }
      System.out.println(ident + "]");
    } else if (value instanceof GenericRecord record) {
      if (field != null) {
        System.out.println(ident + field.name() + " {");
      } else {
        System.out.println(ident + "{");
      }
      for (var f: record.getSchema().getFields()) {
        print(ident + "    ", f, record.get(f.pos()));
      }
      System.out.println(ident + "}");
    } else if (value != null) {
      System.out.println(ident + value.getClass().getSimpleName() + ":" + field.name() + "=" + value);
    } else {
      System.out.println(ident + field.name() + "=" + value);
    }
  }

  private static ParquetReader<GenericRecord> createParquetReader(java.nio.file.Path tmpPath) throws IOException {
    var inputFile = HadoopInputFile.fromPath(new Path(tmpPath.toUri()), new Configuration());
    return AvroParquetReader.<GenericRecord>builder(inputFile)
      .withDataModel(GenericData.get())
      .build();
  }
}
