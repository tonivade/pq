/*
 * Copyright (c) 2023-2024, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static pq.App.createFileReader;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.parquet.filter2.compat.FilterCompat;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "metadata", description = "print metadata of parquet file")
final class MetadataCommand implements Runnable {

  @Parameters(paramLabel = "FILE", description = "parquet file")
  private File file;

  @Option(names = "--show-blocks", description = "show block metadata info", defaultValue = "false")
  private boolean showBlocks;

  @Override
  public void run() {
    try (var reader = createFileReader(file, FilterCompat.NOOP)) {
      reader.getFileMetaData().getKeyValueMetaData()
        .forEach((k, v) -> System.out.println("\"" + k + "\":" + v));
      System.out.println("\"createdBy\":" + reader.getFileMetaData().getCreatedBy());
      System.out.println("\"count\":" + reader.getRecordCount());

      if (showBlocks) {
        for (var block : reader.getFooter().getBlocks()) {
          System.out.println("\"block\":" + block.getOrdinal() + ", \"rowCount\":" + block.getRowCount());
          for (var column : block.getColumns()) {
            System.out.println(
                "\"column\":" + column.getPath() + "," +
                "\"type\":\"" + column.getPrimitiveType() + "\"," +
                "\"index\":" + (column.getColumnIndexReference() != null) + "," +
                "\"dictionary\":" + column.hasDictionaryPage() + "," +
                "\"encrypted\":" + column.isEncrypted() + "," +
                "\"stats\":[" + column.getStatistics() + "]"
            );
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}