/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

final class ParquetOutputFile implements OutputFile {

  private final File file;

  ParquetOutputFile(File file) {
    this.file = requireNonNull(file);
  }

  @Override
  public PositionOutputStream create(long blockSizeHint) throws IOException {
    if (file.exists()) {
      throw new IllegalStateException("file already exists");
    }
    return createOrOverwrite(blockSizeHint);
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
    return new PositionOutputStream() {

      private final OutputStream output = new FileOutputStream(file);

      private long count;

      @Override
      public void write(int b) throws IOException {
        output.write(b);
        count += 1;
      }

      @Override
      public void write(byte[] b) throws IOException {
        output.write(b);
        count += b.length;
      }

      @Override
      public void write(byte[] b, int off, int len) throws IOException {
        output.write(b, off, len);
        count += len;
      }

      @Override
      public long getPos() throws IOException {
        return count;
      }

      @Override
      public void close() throws IOException {
        output.close();
      }
    };
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }
}
