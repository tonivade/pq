package pq.internal;

import java.util.function.Consumer;

import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;

public final class JsonPrimitiveConverterFactory {

  static final class IntegerConverter extends PrimitiveConverter {

    private final Consumer<Integer> consumer;

    public IntegerConverter(Consumer<Integer> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void addInt(int value) {
      consumer.accept(value);
    }
  }

  static final class LongConverter extends PrimitiveConverter {

    private final Consumer<Long> consumer;

    public LongConverter(Consumer<Long> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void addLong(long value) {
      consumer.accept(value);
    }
  }

  static final class FloatConverter extends PrimitiveConverter {

    private final Consumer<Float> consumer;

    public FloatConverter(Consumer<Float> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void addFloat(float value) {
      consumer.accept(value);
    }
  }

  static final class DoubleConverter extends PrimitiveConverter {

    private final Consumer<Double> consumer;

    public DoubleConverter(Consumer<Double> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void addDouble(double value) {
      consumer.accept(value);
    }
  }

  static final class BooleanConverter extends PrimitiveConverter {

    private final Consumer<Boolean> consumer;

    public BooleanConverter(Consumer<Boolean> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void addBoolean(boolean value) {
      consumer.accept(value);
    }
  }

  static final class StringConverter extends PrimitiveConverter {

    private final Consumer<String> consumer;
    private Dictionary dictionary;

    public StringConverter(Consumer<String> consumer) {
      this.consumer = consumer;
    }

    @Override
    public void addBinary(Binary value) {
      consumer.accept(value.toStringUsingUTF8());
    }

    @Override
    public void addValueFromDictionary(int dictionaryId) {
      addBinary(dictionary.decodeToBinary(dictionaryId));
    }

    @Override
    public void setDictionary(Dictionary dictionary) {
      this.dictionary = dictionary;
    }
  }

  public static Object create(Object object) {
    // TODO Auto-generated method stub
    return null;
  }

}
