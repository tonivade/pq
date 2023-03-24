/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static pq.App.createJsonReader;
import static pq.App.createProjection;
import static pq.App.parseFilter;
import static pq.App.schema;
import static pq.App.stream;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.parquet.schema.MessageType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "count", description = "print total number of rows in parquet file")
final class CountCommand implements Runnable {

  @Parameters(paramLabel = "FILE", description = "parquet file")
  private File file;

  @Option(names = "--filter",
      description = "predicate to apply to the rows",
      paramLabel = "PREDICATE")
  private String filter;

  @Override
  public void run() {
    var schema = schema(file);
    var parseFilter = parseFilter(filter, schema);
    var projection = createProjection(schema, filter).orElseGet(() -> justOneColumn(schema));
    try (var reader = createJsonReader(file, parseFilter, projection)) {
      var count = stream(reader).count();
      System.out.println(count);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private MessageType justOneColumn(MessageType schema) {
    return new MessageType(schema.getName(), schema.getFields().get(0));
  }
}