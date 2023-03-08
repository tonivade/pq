/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

class FilterParserTest {

  static final String ID = "id";

  final FilterParser parser = new FilterParser();

  @Test
  void filterIntColumn() {
    assertThat(parser.parse("id = 1")).isEqualTo(eq(intColumn(ID), 1));
    assertThat(parser.parse("id > 1")).isEqualTo(gt(intColumn(ID), 1));
    assertThat(parser.parse("id < 1")).isEqualTo(lt(intColumn(ID), 1));
  }

  @Test
  void filterBooleanColumn() {
    assertThat(parser.parse("id = true")).isEqualTo(eq(booleanColumn(ID), true));
    assertThatThrownBy(() -> parser.parse("id > true")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id < false")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void filterStringColumn() {
    assertThat(parser.parse("id = \"a\"")).isEqualTo(eq(binaryColumn(ID), Binary.fromString("a")));
    assertThatThrownBy(() -> parser.parse("id > \"a\"")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> parser.parse("id < \"a\"")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void filterTwoExpressions() {
    assertThat(parser.parse("id > 2 & id < 10")).isEqualTo(and(gt(intColumn(ID), 2), lt(intColumn(ID), 10)));
    assertThat(parser.parse("id > 2 | id < 10")).isEqualTo(or(gt(intColumn(ID), 2), lt(intColumn(ID), 10)));
  }

  @Test
  void filterThreeExpressions() {
    assertThat(parser.parse("id > 2 & id < 10 | id = 0"))
      .isEqualTo(or(and(gt(intColumn(ID), 2), lt(intColumn(ID), 10)), eq(intColumn(ID), 0)));
  }

}
