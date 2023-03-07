///usr/bin/env jbang "$0" "$@" ; exit $?

//DEPS org.apache.parquet:parquet-avro:1.12.3
//DEPS com.jerolba:carpet-filestream:0.0.3
//DEPS org.apache.hadoop:hadoop-common:3.3.4
//DEPS org.apache.hadoop:hadoop-mapreduce-client-core:3.3.4
//DEPS info.picocli:picocli:4.7.1
//DEPS com.github.petitparser:petitparser-core:2.3.1

/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.filter2.compat.FilterCompat.get;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.petitparser.parser.primitive.CharacterParser.digit;
import static org.petitparser.parser.primitive.CharacterParser.letter;
import static org.petitparser.parser.primitive.CharacterParser.of;
import static org.petitparser.parser.primitive.CharacterParser.word;
import com.jerolba.carpet.filestream.FileSystemInputFile;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.api.Binary;
import org.petitparser.parser.Parser;
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
    @Option(names = "--filter", description = "predicate to apply to the rows", paramLabel = "PREDICATE")
    private String filter;

    @Option(names = "--index", description = "print row index", defaultValue = "false")
    private boolean index;

    @Parameters(paramLabel = "FILE", description = "parquet file")
    private File file;

    private final Output output = new Output(System.out);

    private final FilterParser parser = new FilterParser();

    @Override
    public void run() {
      long size = size();
      FilterPredicate predicate = parser.parse(filter);
      try (var reader = createParquetReader(file, predicate)) {
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

    private long size() {
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

  private static Stream<Tuple> stream(long size, ParquetReader<GenericRecord> reader) {
    var spliterator = Spliterators.spliterator(new ParquetIterator(reader), size,
      Spliterator.ORDERED | Spliterator.NONNULL | Spliterator.IMMUTABLE);
    return StreamSupport.stream(spliterator, false);
  }

  private static ParquetFileReader createFileReader(File file) throws IOException {
    return new ParquetFileReader(new FileSystemInputFile(file), ParquetReadOptions.builder().build());
  }

  private static ParquetReader<GenericRecord> createParquetReader(File file, FilterPredicate filter) throws IOException {
    if (filter != null) {
      return AvroParquetReader.<GenericRecord>builder(new FileSystemInputFile(file))
        .withDataModel(GenericData.get())
        .withFilter(get(filter))
        .build();
    }
    return AvroParquetReader.genericRecordReader(new FileSystemInputFile(file));
  }
}

final class ParquetIterator implements Iterator<Tuple> {

  private final ParquetReader<GenericRecord> reader;

  private GenericRecord current = null;

  public ParquetIterator(ParquetReader<GenericRecord> reader) {
    this.reader = requireNonNull(reader);
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

/*
 * currently only supports one expresion of type "identifier" "=|>|<" "number|string"
 */
final class FilterParser {

  static final Parser ID = letter().seq(letter().or(digit()).star()).flatten();
  static final Parser NUMBER = digit().plus().flatten()
    .map((String n) -> Integer.parseInt(n));
  static final Parser STRING = of('"').seq(word().plus()).seq(of('"')).flatten()
    .map((String s) -> s.replace('"', ' ').trim());
  static final Parser OPERATOR = of('=').or(of('>')).or(of('<')).flatten().trim()
    .map((String o) -> switch (o) {
      case "=" -> Operator.EQUAL;
      case ">" -> Operator.GREATER_THAN;
      case "<" -> Operator.LOWER_THAN;
      default -> throw new IllegalArgumentException("operator not supported: `" + o + "`");
    });
  static final Parser PARSER = ID.seq(OPERATOR).seq(NUMBER)
    .map((List<Object> result) -> {
      String column = (String) result.get(0);
      Operator operator = (Operator) result.get(1);
      Object value = result.get(2);
      return translate(column, operator, value);
    });

  enum Operator {
    EQUAL,
    GREATER_THAN,
    LOWER_THAN
  }

  // FIXME: implement complete syntax
  FilterPredicate parse(String filter) {
    if (filter != null) {
      return PARSER.parse(filter).get();
    }
    return null;
  }

  private static FilterPredicate translate(String column, Operator operator, Object value) {
    if (value instanceof Integer i) {
      return switch (operator) {
        case EQUAL -> eq(intColumn(column), i);
        case GREATER_THAN -> gt(intColumn(column), i);
        case LOWER_THAN -> lt(intColumn(column), i);
        default -> throw new IllegalArgumentException();
      };
    }
    if (value instanceof String s) {
      return switch (operator) {
        case EQUAL -> eq(binaryColumn(column), Binary.fromString(s));
        default -> throw new IllegalArgumentException("operator not supported: " + operator);
      };
    }
    throw new IllegalArgumentException();
  }
}
