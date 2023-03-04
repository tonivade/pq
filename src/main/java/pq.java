///usr/bin/env jbang "$0" "$@" ; exit $?

//DEPS org.apache.parquet:parquet-avro:1.12.3
//DEPS org.apache.hadoop:hadoop-client:3.3.2

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

public class pq {

  public static void main(String[] args) {
    if (args[0].equals("--parse")) {
      parse(new File(args[1]));
    } else if (args[0].equals("--schema")) {
      schema(new File(args[1]));
    } else if (args[0].equals("--count")) {
      count(new File(args[1]));
    } else {
      System.out.println("ERROR: invalid option " + args[0]);
      System.out.println("--parse <file> : parse and print the content of the file");
      System.out.println("--schema <file> : print parquet file schema");
      System.out.println("--count <file> : print number of elements");
    }
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

  static void print(String ident, Field field, Object value) {
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
