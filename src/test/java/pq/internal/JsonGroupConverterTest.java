/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq.internal;

import static com.eclipsesource.json.Json.array;
import static com.eclipsesource.json.Json.object;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Types.optionalGroup;
import static org.apache.parquet.schema.Types.repeated;
import static org.apache.parquet.schema.Types.repeatedGroup;
import static org.apache.parquet.schema.Types.required;
import static org.apache.parquet.schema.Types.requiredGroup;
import static org.mockito.Mockito.verify;

import com.eclipsesource.json.JsonValue;

import java.util.function.Consumer;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class JsonGroupConverterTest {

  private static final String ID = "id";

  Consumer<JsonValue> consumer = Mockito.mock();

  @Nested
  class required {

    @Test
    void convertPrimitiveInt() {
      var converter = new JsonGroupConverter(requiredGroup().addField(required(INT32).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addInt(1);
      converter.end();

      verify(consumer).accept(object().add(ID, 1));
    }

    @Test
    void convertPrimitiveLong() {
      var converter = new JsonGroupConverter(requiredGroup().addField(required(INT64).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addLong(1);
      converter.end();

      verify(consumer).accept(object().add(ID, 1l));
    }

    @Test
    void convertPrimitiveFloat() {
      var converter = new JsonGroupConverter(requiredGroup().addField(required(FLOAT).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addFloat(1);
      converter.end();

      verify(consumer).accept(object().add(ID, 1f));
    }

    @Test
    void convertPrimitiveDouble() {
      var converter = new JsonGroupConverter(requiredGroup().addField(required(DOUBLE).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addDouble(1);
      converter.end();

      verify(consumer).accept(object().add(ID, 1d));
    }

    @Test
    void convertPrimitiveBoolean() {
      var converter = new JsonGroupConverter(requiredGroup().addField(required(BOOLEAN).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addBoolean(true);
      converter.end();

      verify(consumer).accept(object().add(ID, true));
    }

    @Test
    void convertPrimitiveString() {
      var converter = new JsonGroupConverter(requiredGroup().addField(required(BINARY).as(stringType()).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("a"));
      converter.end();

      verify(consumer).accept(object().add(ID, "a"));
    }

    @Test
    void convertInner() {
      PrimitiveType id = required(BINARY).as(stringType()).named(ID);
      var converter = new JsonGroupConverter(requiredGroup().addField(optionalGroup().addField(id).named("inner")).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asGroupConverter().start();
      converter.getConverter(0).asGroupConverter().getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("a"));
      converter.getConverter(0).asGroupConverter().end();
      converter.end();

      verify(consumer).accept(object().add("inner", object().add(ID, "a")));
    }
  }

  @Nested
  class repeated {

    @Test
    void convertPrimitiveInt() {
      var converter = new JsonGroupConverter(requiredGroup().addField(repeated(INT32).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addInt(1);
      converter.getConverter(0).asPrimitiveConverter().addInt(2);
      converter.getConverter(0).asPrimitiveConverter().addInt(3);
      converter.end();

      verify(consumer).accept(object().add(ID, array().add(1).add(2).add(3)));
    }

    @Test
    void convertPrimitiveLong() {
      var converter = new JsonGroupConverter(requiredGroup().addField(repeated(INT64).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addLong(1);
      converter.getConverter(0).asPrimitiveConverter().addLong(2);
      converter.getConverter(0).asPrimitiveConverter().addLong(3);
      converter.end();

      verify(consumer).accept(object().add(ID, array().add(1l).add(2l).add(3l)));
    }

    @Test
    void convertPrimitiveFloat() {
      var converter = new JsonGroupConverter(requiredGroup().addField(repeated(FLOAT).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addFloat(1);
      converter.getConverter(0).asPrimitiveConverter().addFloat(2);
      converter.getConverter(0).asPrimitiveConverter().addFloat(3);
      converter.end();

      verify(consumer).accept(object().add(ID, array().add(1f).add(2f).add(3f)));
    }

    @Test
    void convertPrimitiveDouble() {
      var converter = new JsonGroupConverter(requiredGroup().addField(repeated(DOUBLE).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addDouble(1);
      converter.getConverter(0).asPrimitiveConverter().addDouble(2);
      converter.getConverter(0).asPrimitiveConverter().addDouble(3);
      converter.end();

      verify(consumer).accept(object().add(ID, array().add(1d).add(2d).add(3d)));
    }

    @Test
    void convertPrimitiveBoolean() {
      var converter = new JsonGroupConverter(requiredGroup().addField(repeated(BOOLEAN).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addBoolean(true);
      converter.getConverter(0).asPrimitiveConverter().addBoolean(false);
      converter.getConverter(0).asPrimitiveConverter().addBoolean(true);
      converter.end();

      verify(consumer).accept(object().add(ID, array().add(true).add(false).add(true)));
    }

    @Test
    void convertPrimitiveString() {
      var converter = new JsonGroupConverter(requiredGroup().addField(repeated(BINARY).as(stringType()).named(ID)).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("a"));
      converter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("b"));
      converter.getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("c"));
      converter.end();

      verify(consumer).accept(object().add(ID, array().add("a").add("b").add("c")));
    }

    @Test
    void convertInner() {
      PrimitiveType id = required(BINARY).as(stringType()).named(ID);
      var converter = new JsonGroupConverter(requiredGroup().addField(repeatedGroup().addField(id).named("inner")).named("item"), consumer);

      converter.start();
      converter.getConverter(0).asGroupConverter().start();
      converter.getConverter(0).asGroupConverter().getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("a"));
      converter.getConverter(0).asGroupConverter().end();
      converter.getConverter(0).asGroupConverter().start();
      converter.getConverter(0).asGroupConverter().getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("b"));
      converter.getConverter(0).asGroupConverter().end();
      converter.getConverter(0).asGroupConverter().start();
      converter.getConverter(0).asGroupConverter().getConverter(0).asPrimitiveConverter().addBinary(Binary.fromString("c"));
      converter.getConverter(0).asGroupConverter().end();
      converter.end();

      verify(consumer).accept(object().add("inner", array().add(object().add(ID, "a")).add(object().add(ID, "b")).add(object().add(ID, "c"))));
    }
  }
}
