/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.assertj.core.api.Assertions.assertThat;
import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

class ConverterTest {

  private static final String INNER = "inner";
  private static final String ID = "id";

  @Test
  void convertInt() {
    var schema = createSchemaFor(Types.required(INT32).named(ID).asPrimitiveType());
    var json = new JsonObject().add(ID, 1);

    GenericRecord record = new Converter(schema).toRecord(json);

    var expected = new GenericData.Record(schema);
    expected.put(ID, 1);
    assertThat(record).isEqualTo(expected);
  }

  @Test
  void convertOptional() {
    var schema = createSchemaFor(Types.optional(INT32).named(ID).asPrimitiveType());
    var json = new JsonObject().add(ID, 1);

    GenericRecord record = new Converter(schema).toRecord(json);

    var expected = new GenericData.Record(schema);
    expected.put(ID, 1);
    assertThat(record).isEqualTo(expected);
  }

  @Test
  void convertNull() {
    var schema = createSchemaFor(Types.optional(INT32).named(ID).asPrimitiveType());
    var json = new JsonObject().add(ID, JsonValue.NULL);

    GenericRecord record = new Converter(schema).toRecord(json);

    var expected = new GenericData.Record(schema);
    expected.put(ID, null);
    assertThat(record).isEqualTo(expected);
  }

  @Test
  void convertLong() {
    var schema = createSchemaFor(Types.required(INT64).named(ID).asPrimitiveType());
    var json = new JsonObject().add(ID, 1);

    GenericRecord record = new Converter(schema).toRecord(json);

    var expected = new GenericData.Record(schema);
    expected.put(ID, 1L);
    assertThat(record).isEqualTo(expected);
  }

  @Test
  void convertFloat() {
    var schema = createSchemaFor(Types.required(FLOAT).named(ID).asPrimitiveType());
    var json = new JsonObject().add(ID, 1);

    GenericRecord record = new Converter(schema).toRecord(json);

    var expected = new GenericData.Record(schema);
    expected.put(ID, 1f);
    assertThat(record).isEqualTo(expected);
  }

  @Test
  void convertDouble() {
    var schema = createSchemaFor(Types.required(DOUBLE).named(ID).asPrimitiveType());
    var json = new JsonObject().add(ID, 1);

    GenericRecord record = new Converter(schema).toRecord(json);

    var expected = new GenericData.Record(schema);
    expected.put(ID, 1d);
    assertThat(record).isEqualTo(expected);
  }

  @Test
  void convertBoolean() {
    var schema = createSchemaFor(Types.required(BOOLEAN).named(ID).asPrimitiveType());
    var json = new JsonObject().add(ID, true);

    GenericRecord record = new Converter(schema).toRecord(json);

    var expected = new GenericData.Record(schema);
    expected.put(ID, true);
    assertThat(record).isEqualTo(expected);
  }

  @Test
  void convertString() {
    var schema = createSchemaFor(Types.required(BINARY).as(stringType()).named(ID).asPrimitiveType());
    var json = new JsonObject().add(ID, "hola");

    GenericRecord record = new Converter(schema).toRecord(json);

    var expected = new GenericData.Record(schema);
    expected.put(ID, "hola");
    assertThat(record).isEqualTo(expected);
  }

  @Test
  void convertObject() {
    var schema = createSchemaFor(Types.requiredGroup().addField(Types.required(BINARY).as(stringType()).named(ID).asPrimitiveType()).named(INNER).asGroupType());
    var inner = new JsonObject().add(ID, "hola");
    var json = new JsonObject().add(INNER, inner);

    GenericRecord record = new Converter(schema).toRecord(json);

    var expected = new GenericData.Record(schema);
    var expectedInner = new GenericData.Record(schema.getField(INNER).schema());
    expectedInner.put(ID, "hola");
    expected.put(INNER, expectedInner);
    assertThat(record).isEqualTo(expected);
  }

  @Test
  void convertArray() {
    var schema = createSchemaFor(Types.requiredList().element(Types.required(BINARY).as(stringType()).named(ID).asPrimitiveType()).named("array"));
    var element = new JsonObject().add(ID, "hola");
    var array = new JsonArray().add(element);
    var json = new JsonObject().add("array", array);

    GenericRecord record = new Converter(schema).toRecord(json);

    var expected = new GenericData.Record(schema);
    var expectedArray = new GenericData.Array<>(1, schema.getField("array").schema());
    var expectedElement = new GenericData.Record(schema.getField("array").schema().getElementType());
    expectedElement.put(ID, "hola");
    expectedArray.add(expectedElement);
    expected.put("array", expectedArray);
    assertThat(record).isEqualTo(expected);
  }

  private Schema createSchemaFor(Type integer) {
    return new AvroSchemaConverter().convert(new MessageType("schema", List.of(integer)));
  }
}
