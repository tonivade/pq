/*
 * Copyright (c) 2023-2024, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.util.Objects.requireNonNull;

import com.eclipsesource.json.JsonValue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.parquet.hadoop.ParquetReader;

final class ParquetIterator implements Iterator<Tuple> {

  private final ParquetReader<JsonValue> reader;

  private JsonValue current = null;

  public ParquetIterator(ParquetReader<JsonValue> reader) {
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

  private JsonValue tryAdvance() {
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

