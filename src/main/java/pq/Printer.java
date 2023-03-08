/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.util.Objects.requireNonNull;

import java.io.PrintStream;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericRecord;

final class Printer {

  final PrintStream out;

  Printer(PrintStream out) {
    this.out = requireNonNull(out);
  }

  void printIndex(long index) {
    out.println("#" + index);
  }

  void printObject(GenericRecord value) {
    print(null, value, true);
  }

  void printSeparator() {
    out.println();
  }

  private void print(Field field, Object value, boolean last) {
    if (value instanceof GenericArray<?> array) {
      printArray(field, array, last);
    } else if (value instanceof GenericRecord record) {
      printRecord(field, record, last);
    } else if (value instanceof CharSequence string) {
      printString(field, string, last);
    } else {
      printNotString(field, value, last);
    }
  }

  private void printArray(Field field, GenericArray<?> array, boolean last) {
    printField(field);
    out.print("[");
    int i = 0;
    for (var element : array) {
      print(null, element, array.size() == ++i);
    }
    out.print("]");
    printComma(last);
  }

  private void printRecord(Field field, GenericRecord record, boolean last) {
    printField(field);
    out.print("{");
    int i = 0;
    for (var f: record.getSchema().getFields()) {
      print(f, record.get(f.pos()), record.getSchema().getFields().size() == ++i);
    }
    out.print("}");
    printComma(last);
  }

  private void printString(Field field, CharSequence value, boolean last) {
    printField(field);
    out.print("\"" + value + "\"");
    printComma(last);
  }

  private void printNotString(Field field, Object value, boolean last) {
    printField(field);
    out.print(value);
    printComma(last);
  }

  private void printField(Field field) {
    if (field != null) {
      out.print("\"" + field.name() + "\":");
    }
  }

  private void printComma(boolean last) {
    if (!last) {
      out.print(",");
    }
  }
}

