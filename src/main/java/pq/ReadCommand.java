/*
 * Copyright (c) 2023-2024, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.util.stream.Collectors.joining;
import static pq.App.createJsonReader;
import static pq.App.createProjection;
import static pq.App.parseFilter;
import static pq.App.schema;
import static pq.App.stream;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.parquet.schema.MessageType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "read", description = "print content of parquet file in json format")
final class ReadCommand implements Runnable {

  @Option(names = "--head", description = "get the first N number of rows", paramLabel = "ROWS", defaultValue = "0")
  private int head;

  @Option(names = "--tail", description = "get the last N number of rows", paramLabel = "ROWS", defaultValue = "0")
  private int tail;

  @Option(names = "--get", description = "print just the row with given index", paramLabel = "ROW", defaultValue = "-1")
  private int get;

  @Option(names = "--skip", description = "skip a number N of rows", paramLabel = "ROWS", defaultValue = "0")
  private int skip;

  @Nullable
  @Option(names = "--filter", description = "predicate to apply to the rows", paramLabel = "PREDICATE")
  private String filter;

  @Nullable
  @Option(names = "--select", description = "list of columns to select", paramLabel = "COLUMN", split = ",")
  private String[] select;

  @Option(names = "--index", description = "print row index", defaultValue = "false")
  private boolean index;

  @SuppressWarnings("NullAway.Init")
  @Option(names = "--format", description = "output format, json or csv", defaultValue = "json", paramLabel = "JSON|CSV", converter = FormatConverter.class)
  private Format format;

  @SuppressWarnings("NullAway.Init")
  @Parameters(paramLabel = "FILE", description = "parquet file")
  private File file;

  @Override
  public void run() {
    var schema = schema(file);
    var projection = createProjection(schema, select);
    var output = createOutput(projection.orElse(schema));
    try (var reader = createJsonReader(file, parseFilter(filter, schema), projection.orElse(null))) {
      if (head > 0) {
        stream(reader).skip(skip).limit(head).forEach(output::printRow);
      } else if (tail > 0) {
        var deque = new ArrayDeque<Tuple>(tail);
        stream(reader).skip(skip).forEach(i -> {
          if (deque.size() == tail) {
            deque.removeFirst();
          }
          deque.addLast(i);
        });
        deque.forEach(output::printRow);
      } else if (get > -1) {
        stream(reader).skip(skip).skip(get).findFirst().ifPresent(output::printRow);
      } else {
        stream(reader).skip(skip).forEach(output::printRow);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Output createOutput(MessageType schema) {
    return switch(format) {
      case CSV -> new CsvOutput(schema).printHeader();
      case JSON -> {
        if (index) {
          yield new JsonOutputWithIndex();
        } else {
          yield new JsonOutput();
        }
      }
    };
  }

  interface Output {
    void printRow(Tuple tuple);
  }

  static final class JsonOutput implements Output {
    @Override
    public void printRow(Tuple tuple) {
      System.out.println(tuple.value());
    }
  }

  static final class JsonOutputWithIndex implements Output {
    @Override
    public void printRow(Tuple tuple) {
      System.out.println("#" + tuple.index());
      System.out.println(tuple.value());
    }
  }

  static final class CsvOutput implements Output {

    private final List<String> columns;

    CsvOutput(MessageType schema) {
      List<String> names = new ArrayList<>();
      for (int i = 0; i < schema.getFieldCount(); i++) {
        names.add(schema.getFieldName(i));
      }
      this.columns = List.copyOf(names);
    }

    @Override
    public void printRow(Tuple tuple) {
      List<String> values = new ArrayList<>();
      for (String column : columns) {
        var value = tuple.value().asObject().get(column);
        if (value.isNull()) {
          values.add("");
        } else {
          values.add(value.toString());
        }
      }
      System.out.println(values.stream().collect(joining(",")));
    }

    private CsvOutput printHeader() {
      System.out.println(columns.stream().collect(joining(",")));
      return this;
    }
  }
}