/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
  void count() {
    assertThatThrownBy(() -> pq.main(COUNT, EXAMPLE_PARQUET))
      .isInstanceOf(AbortExecutionException.class);

    assertThat(systemOut.getText()).isEqualTo("""
      1000
      """);
  }

  @Test
  void schema() {
    assertThatThrownBy(() -> pq.main(SCHEMA, EXAMPLE_PARQUET))
      .isInstanceOf(AbortExecutionException.class);

    assertThat(systemOut.getText()).isEqualTo("""
      {"type":"record","name":"spark_schema","fields":[{"name":"id","type":["null","int"],"default":null},{"name":"first_name","type":["null","string"],"default":null},{"name":"last_name","type":["null","string"],"default":null},{"name":"email","type":["null","string"],"default":null},{"name":"gender","type":["null","string"],"default":null},{"name":"ip_address","type":["null","string"],"default":null},{"name":"cc","type":["null","string"],"default":null},{"name":"country","type":["null","string"],"default":null},{"name":"birthdate","type":["null","string"],"default":null},{"name":"salary","type":["null","double"],"default":null},{"name":"title","type":["null","string"],"default":null},{"name":"comments","type":["null","string"],"default":null}]}
      """);
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
    void getWithCounter() {
      assertThatThrownBy(() -> pq.main(READ, "--index", "--get", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #1
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void limit() {
      assertThatThrownBy(() -> pq.main(READ, "--limit", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":1,"first_name":"Amanda","last_name":"Jordan","email":"ajordan0@com.com","gender":"Female","ip_address":null,"cc":"6759521864920116","country":"Indonesia","birthdate":"3/8/1971","salary":49756.53,"title":"Internal Auditor","comments":"1E+02"}
          """);
    }

    @Test
    void limitWithCounter() {
      assertThatThrownBy(() -> pq.main(READ, "--index", "--limit", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #0
          {"id":1,"first_name":"Amanda","last_name":"Jordan","email":"ajordan0@com.com","gender":"Female","ip_address":null,"cc":"6759521864920116","country":"Indonesia","birthdate":"3/8/1971","salary":49756.53,"title":"Internal Auditor","comments":"1E+02"}
          """);
    }

    @Test
    void skipAndLimit() {
      assertThatThrownBy(() -> pq.main(READ, "--skip", "1", "--limit", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void skipAndLimitWithCounter() {
      assertThatThrownBy(() -> pq.main(READ, "--index", "--skip", "1", "--limit", "1", EXAMPLE_PARQUET))
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
    void skipAndGetWithCounter() {
      assertThatThrownBy(() -> pq.main(READ, "--index", "--skip", "1", "--get", "0", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #1
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }
  }
}
