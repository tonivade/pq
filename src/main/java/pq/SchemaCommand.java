/*
 * Copyright (c) 2023-2024, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq;

import static pq.App.createFileReader;
import static pq.App.createProjection;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import javax.annotation.Nullable;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.schema.MessageType;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "schema", description = "print schema of parquet file")
final class SchemaCommand implements Runnable {

  @SuppressWarnings("NullAway.Init")
  @Parameters(paramLabel = "FILE", description = "parquet file")
  private File file;

  @Nullable
  @Option(names = "--select", description = "list of columns to select", paramLabel = "COLUMN", split = ",")
  private String[] select;

  @Override
  public void run() {
    try (var reader = createFileReader(file, FilterCompat.NOOP)) {
      MessageType schema = reader.getFileMetaData().getSchema();
      var projection = createProjection(schema, select).orElse(schema);
      System.out.print(projection);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}