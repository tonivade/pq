/*
 * Copyright (c) 2023-2025, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.doubleColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.longColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
import static org.apache.parquet.filter2.predicate.FilterApi.not;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;

class FilterParserTest {

  static final String ID = "id";

  final FilterParser parser = new FilterParser();

  @Test
  void filterIntColumn() {
    var schema = new MessageType("schema", List.of(new PrimitiveType(REQUIRED, INT32, ID)));
    assertThat(parser.parse("id == 1").apply(schema).convert()).isEqualTo(eq(intColumn(ID), 1));
    assertThat(parser.parse("id == null").apply(schema).convert()).isEqualTo(eq(intColumn(ID), null));
    assertThat(parser.parse("id > 1").apply(schema).convert()).isEqualTo(gt(intColumn(ID), 1));
    assertThat(parser.parse("id < 1").apply(schema).convert()).isEqualTo(lt(intColumn(ID), 1));
    assertThat(parser.parse("id != 1").apply(schema).convert()).isEqualTo(notEq(intColumn(ID), 1));
    assertThat(parser.parse("id >= 1").apply(schema).convert()).isEqualTo(gtEq(intColumn(ID), 1));
    assertThat(parser.parse("id <= 1").apply(schema).convert()).isEqualTo(ltEq(intColumn(ID), 1));
    assertThatThrownBy(() -> parser.parse("id == 1.").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id == true").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id == \"\"").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void filterLongColumn() {
    var schema = new MessageType("schema", List.of(new PrimitiveType(REQUIRED, INT64, ID)));
    assertThat(parser.parse("id == 1").apply(schema).convert()).isEqualTo(eq(longColumn(ID), 1L));
    assertThat(parser.parse("id == null").apply(schema).convert()).isEqualTo(eq(longColumn(ID), null));
    assertThat(parser.parse("id > 1").apply(schema).convert()).isEqualTo(gt(longColumn(ID), 1L));
    assertThat(parser.parse("id < 1").apply(schema).convert()).isEqualTo(lt(longColumn(ID), 1L));
    assertThat(parser.parse("id != 1").apply(schema).convert()).isEqualTo(notEq(longColumn(ID), 1L));
    assertThat(parser.parse("id >= 1").apply(schema).convert()).isEqualTo(gtEq(longColumn(ID), 1L));
    assertThat(parser.parse("id <= 1").apply(schema).convert()).isEqualTo(ltEq(longColumn(ID), 1L));
    assertThatThrownBy(() -> parser.parse("id == 1.").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id == true").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id == \"\"").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void filterFloatColumn() {
    var schema = new MessageType("schema", List.of(new PrimitiveType(REQUIRED, FLOAT, ID)));
    assertThat(parser.parse("id == 1.").apply(schema).convert()).isEqualTo(eq(floatColumn(ID), 1.0f));
    assertThat(parser.parse("id == null").apply(schema).convert()).isEqualTo(eq(floatColumn(ID), null));
    assertThat(parser.parse("id > 1.").apply(schema).convert()).isEqualTo(gt(floatColumn(ID), 1.0f));
    assertThat(parser.parse("id < 1.").apply(schema).convert()).isEqualTo(lt(floatColumn(ID), 1.0f));
    assertThat(parser.parse("id != 1.").apply(schema).convert()).isEqualTo(notEq(floatColumn(ID), 1.0f));
    assertThat(parser.parse("id >= 1.").apply(schema).convert()).isEqualTo(gtEq(floatColumn(ID), 1.0f));
    assertThat(parser.parse("id <= 1.").apply(schema).convert()).isEqualTo(ltEq(floatColumn(ID), 1.0f));
  }

  @Test
  void filterDoubleColumn() {
    var schema = new MessageType("schema", List.of(new PrimitiveType(REQUIRED, DOUBLE, ID)));
    assertThat(parser.parse("id == 1.").apply(schema).convert()).isEqualTo(eq(doubleColumn(ID), 1.0d));
    assertThat(parser.parse("id == null").apply(schema).convert()).isEqualTo(eq(doubleColumn(ID), null));
    assertThat(parser.parse("id > 1.").apply(schema).convert()).isEqualTo(gt(doubleColumn(ID), 1.0d));
    assertThat(parser.parse("id < 1.").apply(schema).convert()).isEqualTo(lt(doubleColumn(ID), 1.0d));
    assertThat(parser.parse("id != 1.").apply(schema).convert()).isEqualTo(notEq(doubleColumn(ID), 1.0d));
    assertThat(parser.parse("id >= 1.").apply(schema).convert()).isEqualTo(gtEq(doubleColumn(ID), 1.0d));
    assertThat(parser.parse("id <= 1.").apply(schema).convert()).isEqualTo(ltEq(doubleColumn(ID), 1.0d));
  }

  @Test
  void filterBooleanColumn() {
    var schema = new MessageType("schema", List.of(new PrimitiveType(REQUIRED, BOOLEAN, ID)));
    assertThat(parser.parse("id == true").apply(schema).convert()).isEqualTo(eq(booleanColumn(ID), true));
    assertThat(parser.parse("id == null").apply(schema).convert()).isEqualTo(eq(booleanColumn(ID), null));
    assertThat(parser.parse("id != false").apply(schema).convert()).isEqualTo(notEq(booleanColumn(ID), false));
    assertThat(parser.parse("id").apply(schema).convert()).isEqualTo(eq(booleanColumn(ID), true));
    assertThat(parser.parse("!id").apply(schema).convert()).isEqualTo(notEq(booleanColumn(ID), true));
    assertThatThrownBy(() -> parser.parse("id > true").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id < false").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id >= true").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id <= false").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void filterStringColumn() {
    var schema = new MessageType("schema", List.of(new PrimitiveType(REQUIRED, BINARY, ID)));
    assertThat(parser.parse("id == \"a\"").apply(schema).convert()).isEqualTo(eq(binaryColumn(ID), Binary.fromString("a")));
    assertThat(parser.parse("id == \"\"").apply(schema).convert()).isEqualTo(eq(binaryColumn(ID), Binary.fromString("")));
    assertThat(parser.parse("id == null").apply(schema).convert()).isEqualTo(eq(binaryColumn(ID), null));
    assertThat(parser.parse("id != \"a\"").apply(schema).convert()).isEqualTo(notEq(binaryColumn(ID), Binary.fromString("a")));
    assertThatThrownBy(() -> parser.parse("id > \"a\"").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id < \"a\"").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id >= \"a\"").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id <= \"a\"").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void filterInnerColumn() {
    var inner = new GroupType(REQUIRED, "inner", List.of(new PrimitiveType(REQUIRED, INT32, ID)));
    var schema = new MessageType("schema", List.of(inner));
    assertThat(parser.parse("inner.id == 1").apply(schema).convert()).isEqualTo(eq(intColumn("inner." + ID), 1));
    assertThat(parser.parse("inner.id == null").apply(schema).convert()).isEqualTo(eq(intColumn("inner." + ID), null));
    assertThat(parser.parse("inner.id > 1").apply(schema).convert()).isEqualTo(gt(intColumn("inner." + ID), 1));
    assertThat(parser.parse("inner.id < 1").apply(schema).convert()).isEqualTo(lt(intColumn("inner." + ID), 1));
    assertThat(parser.parse("inner.id != 1").apply(schema).convert()).isEqualTo(notEq(intColumn("inner." + ID), 1));
    assertThat(parser.parse("inner.id >= 1").apply(schema).convert()).isEqualTo(gtEq(intColumn("inner." + ID), 1));
    assertThat(parser.parse("inner.id <= 1").apply(schema).convert()).isEqualTo(ltEq(intColumn("inner." + ID), 1));
    assertThatThrownBy(() -> parser.parse("inner.id == 1.").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("inner.id == true").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("inner.id == \"\"").apply(schema).convert()).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void filterTwoExpressions() {
    var schema = new MessageType("schema", List.of(new PrimitiveType(REQUIRED, INT32, ID)));
    assertThat(parser.parse("id > 2 && id < 10").apply(schema).convert())
      .isEqualTo(and(gt(intColumn(ID), 2), lt(intColumn(ID), 10)));
    assertThat(parser.parse("id > 2 || id < 10").apply(schema).convert())
      .isEqualTo(or(gt(intColumn(ID), 2), lt(intColumn(ID), 10)));
  }

  @Test
  void filterThreeExpressions() {
    var schema = new MessageType("schema", List.of(new PrimitiveType(REQUIRED, INT32, ID)));
    assertThat(parser.parse("(id > 2 && id < 10) || id == 0").apply(schema).convert())
      .isEqualTo(or(and(gt(intColumn(ID), 2), lt(intColumn(ID), 10)), eq(intColumn(ID), 0)));
  }

  @Test
  void filterThreeExpressionsWithParent() {
    var schema = new MessageType("schema", List.of(new PrimitiveType(REQUIRED, INT32, ID)));
    assertThat(parser.parse("id > 2 && (id < 10 || id == 0)").apply(schema).convert())
      .isEqualTo(and(gt(intColumn(ID), 2), or(lt(intColumn(ID), 10), eq(intColumn(ID), 0))));
  }

  @Test
  void notExpression() {
    var schema = new MessageType("schema", List.of(new PrimitiveType(REQUIRED, INT32, ID)));

    FilterPredicate parse = parser.parse("!(id == null)").apply(schema).convert();

    assertThat(parse).isEqualTo(not(eq(intColumn(ID), null)));
  }

}
