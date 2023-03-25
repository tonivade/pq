/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.util.Objects.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

final class ParquetInputFile implements InputFile {

  private static final int BUFFER_LENGTH = 8 * 1024;

  private final File file;

  ParquetInputFile(File file) {
    this.file = requireNonNull(file);
  }

  @Override
  public long getLength() throws IOException {
    return file.length();
  }

  @Override
  public SeekableInputStream newStream() throws IOException {
    return new SeekableInputStream() {

      private final RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r");

      @Override
      public long getPos() throws IOException {
        return randomAccessFile.getFilePointer();
      }

      @Override
      public int read() throws IOException {
        return randomAccessFile.read();
      }

      @Override
      public int read(byte[] b) throws IOException {
        return randomAccessFile.read(b);
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
        return randomAccessFile.read(b, off, len);
      }

      @Override
      public int read(ByteBuffer buf) throws IOException {
        int total = 0;
        byte[] buffer = new byte[BUFFER_LENGTH];
        while (buf.remaining() > 0) {
          int read = randomAccessFile.read(buffer, 0, Math.min(buf.remaining(), buffer.length));
          buf.put(buffer, 0, read);
          total += read;
        }
        return total;
      }

      @Override
      public void readFully(byte[] bytes, int start, int len) throws IOException {
        randomAccessFile.readFully(bytes, start, len);
      }

      @Override
      public void readFully(ByteBuffer buf) throws IOException {
        byte[] buffer = new byte[BUFFER_LENGTH];
        while (buf.remaining() > 0) {
          int read = randomAccessFile.read(buffer, 0, Math.min(buf.remaining(), buffer.length));
          buf.put(buffer, 0, read);
        }
      }

      @Override
      public void readFully(byte[] bytes) throws IOException {
        randomAccessFile.readFully(bytes);
      }

      @Override
      public void seek(long newPos) throws IOException {
        randomAccessFile.seek(newPos);
      }

      @Override
      public void close() throws IOException {
        randomAccessFile.close();
      }
    };
  }
}
