/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.parquet.hadoop.ParquetReader;

final class ParquetIterator<T> implements Iterator<Tuple<T>> {

  private final ParquetReader<T> reader;

  private T current = null;

  public ParquetIterator(ParquetReader<T> reader) {
    this.reader = requireNonNull(reader);
  }

  @Override
  public boolean hasNext() {
    return tryAdvance() != null;
  }

  @Override
  public Tuple<T> next() {
    var result = tryAdvance();
    if (result == null) {
      throw new NoSuchElementException();
    }
    current = null;
    return new Tuple<>(reader.getCurrentRowIndex(), result);
  }

  private T tryAdvance() {
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

record Tuple<T>(long index, T value) {}

