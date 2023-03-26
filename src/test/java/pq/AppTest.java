/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import uk.org.webcompere.systemstubs.jupiter.SystemStub;
import uk.org.webcompere.systemstubs.jupiter.SystemStubsExtension;
import uk.org.webcompere.systemstubs.security.AbortExecutionException;
import uk.org.webcompere.systemstubs.security.SystemExit;
import uk.org.webcompere.systemstubs.stream.SystemIn;
import uk.org.webcompere.systemstubs.stream.SystemOut;

@ExtendWith(SystemStubsExtension.class)
class AppTest {

  private static final String READ = "read";
  private static final String SCHEMA = "schema";
  private static final String COUNT = "count";

  private static final String EXAMPLE_PARQUET = "src/test/resources/example.parquet";

  @SystemStub
  SystemExit systemExit;

  @SystemStub
  SystemOut systemOut;

  @SystemStub
  SystemIn systemIn;

  @Nested
  class schema {

    @Test
    void parquet() {
      assertThatThrownBy(() -> App.main(SCHEMA, EXAMPLE_PARQUET))
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

    @Test
    void parquetWithSelect() {
      assertThatThrownBy(() -> App.main(SCHEMA, "--select", "id,first_name", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
        message spark_schema {
          optional int32 id;
          optional binary first_name (STRING);
        }
        """);
    }
  }

  @Nested
  class count {

    @Test
    void countWithoutFilter() {
      assertThatThrownBy(() -> App.main(COUNT, EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
        1000
        """);
    }

    @Test
    void countWithFilter() {
      assertThatThrownBy(() -> App.main(COUNT, "--filter", "gender == \"Female\"", EXAMPLE_PARQUET))
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
      assertThatThrownBy(() -> App.main(READ, "--get", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void csvFormat() {
      assertThatThrownBy(() -> App.main(READ, "--format", "csv", "--get", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          id,first_name,last_name,email,gender,ip_address,cc,country,birthdate,salary,title,comments
          2,"Albert","Freeman","afreeman1@is.gd","Male","218.111.175.34","","Canada","1/16/1968",150280.17,"Accountant IV",""
          """);
    }

    @Test
    void getWithIndex() {
      assertThatThrownBy(() -> App.main(READ, "--index", "--get", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #1
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void head() {
      assertThatThrownBy(() -> App.main(READ, "--head", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":1,"first_name":"Amanda","last_name":"Jordan","email":"ajordan0@com.com","gender":"Female","ip_address":null,"cc":"6759521864920116","country":"Indonesia","birthdate":"3/8/1971","salary":49756.53,"title":"Internal Auditor","comments":"1E+02"}
          """);
    }

    @Test
    void headWithIndex() {
      assertThatThrownBy(() -> App.main(READ, "--index", "--head", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #0
          {"id":1,"first_name":"Amanda","last_name":"Jordan","email":"ajordan0@com.com","gender":"Female","ip_address":null,"cc":"6759521864920116","country":"Indonesia","birthdate":"3/8/1971","salary":49756.53,"title":"Internal Auditor","comments":"1E+02"}
          """);
    }

    @Test
    void skipAndHead() {
      assertThatThrownBy(() -> App.main(READ, "--skip", "1", "--head", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void skipAndHeadWithIndex() {
      assertThatThrownBy(() -> App.main(READ, "--index", "--skip", "1", "--head", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #1
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void skipAndGet() {
      assertThatThrownBy(() -> App.main(READ, "--skip", "1", "--get", "0", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void skipAndGetWithIndex() {
      assertThatThrownBy(() -> App.main(READ, "--index", "--skip", "1", "--get", "0", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #1
          {"id":2,"first_name":"Albert","last_name":"Freeman","email":"afreeman1@is.gd","gender":"Male","ip_address":"218.111.175.34","cc":"","country":"Canada","birthdate":"1/16/1968","salary":150280.17,"title":"Accountant IV","comments":""}
          """);
    }

    @Test
    void tail() {
      assertThatThrownBy(() -> App.main(READ, "--tail", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":1000,"first_name":"Julie","last_name":"Meyer","email":"jmeyerrr@flavors.me","gender":"Female","ip_address":"217.1.147.132","cc":"374288099198540","country":"China","birthdate":"","salary":222561.13,"title":"","comments":""}
          """);
    }

    @Test
    void tailWithIndex() {
      assertThatThrownBy(() -> App.main(READ, "--index", "--tail", "1", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          #999
          {"id":1000,"first_name":"Julie","last_name":"Meyer","email":"jmeyerrr@flavors.me","gender":"Female","ip_address":"217.1.147.132","cc":"374288099198540","country":"China","birthdate":"","salary":222561.13,"title":"","comments":""}
          """);
    }

    @Test
    void filterInt() {
      assertThatThrownBy(() -> App.main(READ, "--filter", "id == 1000", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":1000,"first_name":"Julie","last_name":"Meyer","email":"jmeyerrr@flavors.me","gender":"Female","ip_address":"217.1.147.132","cc":"374288099198540","country":"China","birthdate":"","salary":222561.13,"title":"","comments":""}
          """);
    }

    @Test
    void filterString() {
      assertThatThrownBy(() -> App.main(READ, "--filter", "last_name == \"Meyer\"", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":201,"first_name":"Brian","last_name":"Meyer","email":"bmeyer5k@t-online.de","gender":"Male","ip_address":"85.164.45.115","cc":"","country":"Uganda","birthdate":"7/4/1963","salary":252555.65,"title":"Senior Cost Accountant","comments":""}
          {"id":838,"first_name":"Irene","last_name":"Meyer","email":"imeyern9@ed.gov","gender":"Female","ip_address":"58.245.119.96","cc":"6331103072856450497","country":"Ecuador","birthdate":"5/19/1963","salary":233719.55,"title":"GIS Technical Architect","comments":""}
          {"id":1000,"first_name":"Julie","last_name":"Meyer","email":"jmeyerrr@flavors.me","gender":"Female","ip_address":"217.1.147.132","cc":"374288099198540","country":"China","birthdate":"","salary":222561.13,"title":"","comments":""}
          """);
    }

    @Test
    void select() {
      assertThatThrownBy(() -> App.main(READ, "--select", "id,email", "--head", "3", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":1,"email":"ajordan0@com.com"}
          {"id":2,"email":"afreeman1@is.gd"}
          {"id":3,"email":"emorgan2@altervista.org"}
          """);
    }

    @Test
    void selectCsvFormat() {
      assertThatThrownBy(() -> App.main(READ, "--select", "id,email", "--head", "3", "--format", "csv", EXAMPLE_PARQUET))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          id,email
          1,"ajordan0@com.com"
          2,"afreeman1@is.gd"
          3,"emorgan2@altervista.org"
          """);
    }
  }

  @Nested
  class write {

    @Test
    void writeFileCsv() throws IOException {
      var schemaFile = File.createTempFile("test", ".schema");
      Files.writeString(schemaFile.toPath(), """
          message spark_schema {
            optional int32 id;
            optional binary email (STRING);
          }
          """, UTF_8);
      systemIn.setInputStream(new ByteArrayInputStream("""
          1,"ajordan0@com.com"
          2,"afreeman1@is.gd"
          3,"emorgan2@altervista.org"
          """.getBytes()));

      var tempFile = File.createTempFile("test", ".parquet");
      assertThatThrownBy(() -> App.main("write", "--schema", schemaFile.getAbsolutePath(), "--format", "csv", tempFile.getAbsolutePath()))
        .isInstanceOf(AbortExecutionException.class);
      assertThatThrownBy(() -> App.main(READ, tempFile.getAbsolutePath()))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":1,"email":"ajordan0@com.com"}
          {"id":2,"email":"afreeman1@is.gd"}
          {"id":3,"email":"emorgan2@altervista.org"}
          """);
    }

    @Test
    void writeFileJson() throws IOException {
      var schemaFile = File.createTempFile("test", ".schema");
      Files.writeString(schemaFile.toPath(), """
          message spark_schema {
            optional int32 id;
            optional binary email (STRING);
          }
          """, UTF_8);
      systemIn.setInputStream(new ByteArrayInputStream("""
          {"id":1,"email":"ajordan0@com.com"}
          {"id":2,"email":"afreeman1@is.gd"}
          {"id":3,"email":"emorgan2@altervista.org"}
          """.getBytes()));

      var tempFile = File.createTempFile("test", ".parquet");
      assertThatThrownBy(() -> App.main("write", "--schema", schemaFile.getAbsolutePath(), tempFile.getAbsolutePath()))
        .isInstanceOf(AbortExecutionException.class);
      assertThatThrownBy(() -> App.main(READ, tempFile.getAbsolutePath()))
        .isInstanceOf(AbortExecutionException.class);

      assertThat(systemOut.getText()).isEqualTo("""
          {"id":1,"email":"ajordan0@com.com"}
          {"id":2,"email":"afreeman1@is.gd"}
          {"id":3,"email":"emorgan2@altervista.org"}
          """);
    }
  }
}
