/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.security.AbortExecutionException;
import uk.org.webcompere.systemstubs.security.SystemExit;
import uk.org.webcompere.systemstubs.stream.SystemOut;

@ExtendWith(SystemStubsExtension.class)
class test {

  private static final String READ = "read";
  private static final String SCHEMA = "schema";
  private static final String COUNT = "count";

  private static final String EXAMPLE_PARQUET = "src/test/resources/example.parquet";

  @SystemStub
  SystemExit systemExit;

  @SystemStub
  SystemOut systemOut;

  @Test
  void schema() {
    assertThatThrownBy(() -> pq.main(SCHEMA, EXAMPLE_PARQUET))
      .isInstanceOf(AbortExecutionException.class);

    assertThat(systemOut.getText()).isEqualTo("""
      message spark_schema {
        optional int32 id;
        optional binary first_name (STRING);
        optional binary last_name (STRING);
        optional binary email (STRING);
        optional binary gender (STRING);
        optional binary ip_address (STRING);
        optional binary cc (STRING);
        optional binary country (STRING);
        optional binary birthdate (STRING);
        optional double salary;
        optional binary title (STRING);
        optional binary comments (STRING);
      }
      """);
  }

  @Nested
  class count {

    @Test
    void countWithoutFilter() {
      assertThatThrownBy(() -> pq.main(COUNT, EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
        1000
        """);
    }

    @Test
    @Disabled("not working yet")
    void countWithFilter() {
      assertThatThrownBy(() -> pq.main(COUNT, "--filter", "gender = \"Female\"", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
        482
        """);
    }
  }

  @Nested
  class read {

    @Test
    void get() {
      assertThatThrownBy(() -> pq.main(READ, "--get", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void getWithIndex() {
      assertThatThrownBy(() -> pq.main(READ, "--index", "--get", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #1
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void head() {
      assertThatThrownBy(() -> pq.main(READ, "--head", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":1,"first_name":"Amanda","last_name":"Jordan","email":"ajordan0@com.com","gender":"Female","ip_address":null,"cc":"6759521864920116","country":"Indonesia","birthdate":"3/8/1971","salary":49756.53,"title":"Internal Auditor","comments":"1E+02"}
          """);
    }

    @Test
    void headWithIndex() {
      assertThatThrownBy(() -> pq.main(READ, "--index", "--head", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #0
          {"id":1,"first_name":"Amanda","last_name":"Jordan","email":"ajordan0@com.com","gender":"Female","ip_address":null,"cc":"6759521864920116","country":"Indonesia","birthdate":"3/8/1971","salary":49756.53,"title":"Internal Auditor","comments":"1E+02"}
          """);
    }

    @Test
    void skipAndHead() {
      assertThatThrownBy(() -> pq.main(READ, "--skip", "1", "--head", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void skipAndHeadWithIndex() {
      assertThatThrownBy(() -> pq.main(READ, "--index", "--skip", "1", "--head", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #1
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void skipAndGet() {
      assertThatThrownBy(() -> pq.main(READ, "--skip", "1", "--get", "0", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void skipAndGetWithIndex() {
      assertThatThrownBy(() -> pq.main(READ, "--index", "--skip", "1", "--get", "0", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #1
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void tail() {
      assertThatThrownBy(() -> pq.main(READ, "--tail", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":1000,"first_name":"Julie","last_name":"Meyer","email":"jmeyerrr@flavors.me","gender":"Female","ip_address":"217.1.147.132","cc":"374288099198540","country":"China","birthdate":"","salary":222561.13,"title":"","comments":""}
          """);
    }

    @Test
    void tailWithIndex() {
      assertThatThrownBy(() -> pq.main(READ, "--index", "--tail", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #999
          {"id":1000,"first_name":"Julie","last_name":"Meyer","email":"jmeyerrr@flavors.me","gender":"Female","ip_address":"217.1.147.132","cc":"374288099198540","country":"China","birthdate":"","salary":222561.13,"title":"","comments":""}
          """);
    }

    @Test
    void filterInt() {
      assertThatThrownBy(() -> pq.main(READ, "--filter", "id = 1000", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":1000,"first_name":"Julie","last_name":"Meyer","email":"jmeyerrr@flavors.me","gender":"Female","ip_address":"217.1.147.132","cc":"374288099198540","country":"China","birthdate":"","salary":222561.13,"title":"","comments":""}
          """);
    }

    @Test
    void filterString() {
      assertThatThrownBy(() -> pq.main(READ, "--filter", "last_name = \"Meyer\"", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":201,"first_name":"Brian","last_name":"Meyer","email":"bmeyer5k@t-online.de","gender":"Male","ip_address":"85.164.45.115","cc":"","country":"Uganda","birthdate":"7/4/1963","salary":252555.65,"title":"Senior Cost Accountant","comments":""}
          {"id":838,"first_name":"Irene","last_name":"Meyer","email":"imeyern9@ed.gov","gender":"Female","ip_address":"58.245.119.96","cc":"6331103072856450497","country":"Ecuador","birthdate":"5/19/1963","salary":233719.55,"title":"GIS Technical Architect","comments":""}
          {"id":1000,"first_name":"Julie","last_name":"Meyer","email":"jmeyerrr@flavors.me","gender":"Female","ip_address":"217.1.147.132","cc":"374288099198540","country":"China","birthdate":"","salary":222561.13,"title":"","comments":""}
          """);
    }
  }

  @Nested
  class parser {

    static final String ID = "id";

    final FilterParser parser = new FilterParser();

    @Test
    void filterIntColumn() {
      assertThat(parser.parse("id = 1")).isEqualTo(eq(intColumn(ID), 1));
      assertThat(parser.parse("id > 1")).isEqualTo(gt(intColumn(ID), 1));
      assertThat(parser.parse("id < 1")).isEqualTo(lt(intColumn(ID), 1));
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
}
