/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import picocli.CommandLine.ITypeConverter;

final class FormatConverter implements ITypeConverter<Format> {

  @Override
  public Format convert(String value) {
    return Format.valueOf(value.toUpperCase());
  }
}