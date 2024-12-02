/*
 * Copyright (c) 2023-2024, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq;

import static pq.App.parseFilter;
import static pq.App.schema;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import javax.annotation.Nullable;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "count", description = "print total number of rows in parquet file")
final class CountCommand implements Runnable {

  @SuppressWarnings("NullAway.Init")
  @Parameters(paramLabel = "FILE", description = "parquet file")
  private File file;

  @Nullable
  @Option(names = "--filter",
      description = "predicate to apply to the rows",
      paramLabel = "PREDICATE")
  private String filter;

  @Override
  public void run() {
    var schema = schema(file);
    var parseFilter = parseFilter(filter, schema);
    try (var reader = App.createFileReader(file, parseFilter)) {
      var count = reader.getFilteredRecordCount();
      System.out.println(count);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}