/*
 * Copyright (c) 2023-2024, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq;

import java.util.Locale;

import picocli.CommandLine.ITypeConverter;

final class FormatConverter implements ITypeConverter<Format> {

  @Override
  public Format convert(String value) {
    return Format.valueOf(value.toUpperCase(Locale.ROOT));
  }
}