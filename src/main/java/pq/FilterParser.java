/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
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
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.petitparser.parser.primitive.CharacterParser.digit;
import static org.petitparser.parser.primitive.CharacterParser.letter;
import static org.petitparser.parser.primitive.CharacterParser.word;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.petitparser.context.Result;
import org.petitparser.parser.Parser;
import org.petitparser.parser.primitive.CharacterParser;
import org.petitparser.parser.primitive.StringParser;
import org.petitparser.tools.GrammarDefinition;
import org.petitparser.tools.GrammarParser;

import pq.FilterParser.Expr.Condition;
import pq.FilterParser.Expr.Expression;
import pq.FilterParser.Expr.NotExpression;
import pq.FilterParser.Expr.NullExpr;
import pq.FilterParser.TypedExpr.NullTypedExpr;
import pq.FilterParser.TypedExpr.TypedCondition.BooleanCondition;
import pq.FilterParser.TypedExpr.TypedCondition.DoubleCondition;
import pq.FilterParser.TypedExpr.TypedCondition.FloatCondition;
import pq.FilterParser.TypedExpr.TypedCondition.IntCondition;
import pq.FilterParser.TypedExpr.TypedCondition.LongCondition;
import pq.FilterParser.TypedExpr.TypedCondition.StringCondition;
import pq.FilterParser.TypedExpr.TypedExpression;

final class FilterParser extends GrammarDefinition {

  private static final CharacterParser NOT = CharacterParser.of('!');
  private static final CharacterParser LEFTPARENT = CharacterParser.of('(');
  private static final CharacterParser RIGHTPARENT = CharacterParser.of(')');
  private static final CharacterParser PIPE = CharacterParser.of('|');
  private static final CharacterParser AMPERSAND = CharacterParser.of('&');
  private static final CharacterParser LT = CharacterParser.of('<');
  private static final CharacterParser GT = CharacterParser.of('>');
  private static final CharacterParser EQ = CharacterParser.of('=');
  private static final CharacterParser QUOTE = CharacterParser.of('"');
  private static final CharacterParser MINUS = CharacterParser.of('-');
  private static final CharacterParser DOT = CharacterParser.of('.');
  private static final CharacterParser UNDERSCORE = CharacterParser.of('_');

  private static final Parser FALSE = StringParser.of("false");
  private static final Parser TRUE = StringParser.of("true");

  private static final Parser NULL = StringParser.of("null").map(x -> null);

  private static final Parser ID = letter().seq(word().or(UNDERSCORE).or(DOT).star()).flatten();

  private static final Parser BOOLEAN = TRUE.or(FALSE).flatten()
    .<String, Boolean>map(Boolean::parseBoolean);

  private static final Parser INTEGER = MINUS.optional().seq(digit().plus()).flatten()
    .<String, Long>map(Long::parseLong);

  private static final Parser DECIMAL = MINUS.optional().seq(digit().plus()).seq(DOT).seq(digit().star()).flatten()
    .<String, Double>map(Double::parseDouble);

  private static final Parser STRING = QUOTE.seq(word().star()).seq(QUOTE).flatten()
    .<String, String>map(s -> s.replace('"', ' ').trim());

  private static final Parser OPERATOR = EQ.seq(EQ).or(GT.seq(EQ.optional())).or(LT.seq(EQ.optional())).or(NOT.seq(EQ)).flatten().trim()
    .<String, Operator>map(FilterParser::toOperator);

  private static final Parser LOGIC = AMPERSAND.seq(AMPERSAND).or(PIPE.seq(PIPE)).flatten().trim()
    .<String, Logic>map(FilterParser::toLogic);

  @SuppressWarnings("unchecked")
  public FilterParser() {
    def("value", STRING.or(DECIMAL).or(BOOLEAN).or(INTEGER).or(NULL));
    def("singleExpression", ID.seq(OPERATOR).seq(ref("value"))
      .<List<Object>, Expr>map(result -> {
        var column = (String) result.get(0);
        var operator = (Operator) result.get(1);
        var value = result.get(2);
        return new Condition(column, operator, value);
      }));
    def("notExpression", NOT.seq(LEFTPARENT).seq(ref("expression")).seq(RIGHTPARENT)
      .<List<Object>, Expr>map(result -> {
        Expr inner = (Expr) result.get(2);
        return new NotExpression(inner);
      }));
    def("parentExpression", LEFTPARENT.seq(ref("expression")).seq(RIGHTPARENT)
      .<List<Object>, Expr>map(result -> {
        throw new RuntimeException();
      }));
    def("expression", ref("notExpression").or(ref("parentExpression")).or(ref("singleExpression")));
    def("start", ref("expression").seq(LOGIC.seq(ref("expression")).star())
      .<List<Object>, Expr>map(result -> {
        var first = (Expr) result.get(0);
        var second = (List<List<Object>>) result.get(1);
        return reduce(first, second);
      }));
  }

  enum Operator {
    EQUAL,
    NOT_EQUAL,
    GREATER_THAN,
    LOWER_THAN,
    GREATER_THAN_EQUAL,
    LOWER_THAN_EQUAL
  }

  enum Logic {
    AND,
    OR
  }

  sealed interface Expr {

    TypedExpr apply(MessageType schema);

    Set<String> columns();

    record Condition(String column, Operator operator, Object value) implements Expr {
      @Override
      public TypedExpr apply(MessageType schema) {
        String[] path = column.split("\\.");
        if (!schema.containsPath(path)) {
          throw new IllegalArgumentException("field not exists: " + column);
        }

        var columnDescription = schema.getColumnDescription(path);

        return switch (columnDescription.getPrimitiveType().getPrimitiveTypeName()) {
          case INT32 -> new IntCondition(column, operator, asInt());
          case INT64 -> new LongCondition(column, operator, asLong());
          case FLOAT -> new FloatCondition(column, operator, asFloat());
          case DOUBLE -> new DoubleCondition(column, operator, asDouble());
          case BOOLEAN -> new BooleanCondition(column, operator, asBoolean());
          case BINARY -> new StringCondition(column, operator, asString());
          default -> throw new IllegalArgumentException("not supported: " + columnDescription);
        };
      }

      @Override
      public Set<String> columns() {
        return Set.of(column);
      }

      private String asString() {
        return (String) value;
      }

      private Boolean asBoolean() {
        return (Boolean) value;
      }

      private Double asDouble() {
        return (Double) value;
      }

      private Long asLong() {
        return (Long) value;
      }

      private Integer asInt() {
        if (value == null) {
          return null;
        }
        return ((Long) value).intValue();
      }

      private Float asFloat() {
        if (value == null) {
          return null;
        }
        return ((Double) value).floatValue();
      }
    }

    record Expression(Expr left, Logic operator, Expr right) implements Expr {
      @Override
      public TypedExpr apply(MessageType schema) {
        return new TypedExpression(left.apply(schema), operator, right.apply(schema));
      }

      @Override
      public Set<String> columns() {
        Set<String> columns = new HashSet<>();
        columns.addAll(left.columns());
        columns.addAll(right.columns());
        return Set.copyOf(columns);
      }
    }

    record NotExpression(Expr inner) implements Expr {

      @Override
      public TypedExpr apply(MessageType schema) {
        return new TypedExpr.TypedNotExpression(inner.apply(schema));
      }

      @Override
      public Set<String> columns() {
        return inner.columns();
      }
    }

    record NullExpr() implements Expr {
      @Override
      public TypedExpr apply(MessageType schema) {
        return new NullTypedExpr();
      }

      @Override
      public Set<String> columns() {
        return Set.of();
      }
    }
  }

  sealed interface TypedExpr {

    FilterPredicate convert();

    sealed interface TypedCondition<T> extends TypedExpr {

      String column();
      Operator operator();
      T value();

      record IntCondition(String column, Operator operator, Integer value) implements TypedCondition<Integer> {
        @Override
        public FilterPredicate convert() {
          return switch (operator) {
            case EQUAL -> eq(intColumn(column), value);
            case NOT_EQUAL -> notEq(intColumn(column), value);
            case GREATER_THAN -> gt(intColumn(column), value);
            case LOWER_THAN -> lt(intColumn(column), value);
            case GREATER_THAN_EQUAL -> gtEq(intColumn(column), value);
            case LOWER_THAN_EQUAL -> ltEq(intColumn(column), value);
          };
        }
      }

      record LongCondition(String column, Operator operator, Long value) implements TypedCondition<Long> {
        @Override
        public FilterPredicate convert() {
          return switch (operator) {
            case EQUAL -> eq(longColumn(column), value);
            case NOT_EQUAL -> notEq(longColumn(column), value);
            case GREATER_THAN -> gt(longColumn(column), value);
            case LOWER_THAN -> lt(longColumn(column), value);
            case GREATER_THAN_EQUAL -> gtEq(longColumn(column), value);
            case LOWER_THAN_EQUAL -> ltEq(longColumn(column), value);
          };
        }
      }

      record StringCondition(String column, Operator operator, String value) implements TypedCondition<String> {
        @Override
        public FilterPredicate convert() {
          return switch (operator) {
            case EQUAL -> eq(binaryColumn(column), asBinary());
            case NOT_EQUAL -> notEq(binaryColumn(column), asBinary());
            default -> throw new IllegalArgumentException();
          };
        }

        private Binary asBinary() {
          return value != null ? Binary.fromString(value) : null;
        }
      }

      record FloatCondition(String column, Operator operator, Float value) implements TypedCondition<Float> {
        @Override
        public FilterPredicate convert() {
          return switch (operator) {
            case EQUAL -> eq(floatColumn(column), value);
            case NOT_EQUAL -> notEq(floatColumn(column), value);
            case GREATER_THAN -> gt(floatColumn(column), value);
            case LOWER_THAN -> lt(floatColumn(column), value);
            case GREATER_THAN_EQUAL -> gtEq(floatColumn(column), value);
            case LOWER_THAN_EQUAL -> ltEq(floatColumn(column), value);
          };
        }
      }

      record DoubleCondition(String column, Operator operator, Double value) implements TypedCondition<Double> {
        @Override
        public FilterPredicate convert() {
          return switch (operator) {
            case EQUAL -> eq(doubleColumn(column), value);
            case NOT_EQUAL -> notEq(doubleColumn(column), value);
            case GREATER_THAN -> gt(doubleColumn(column), value);
            case LOWER_THAN -> lt(doubleColumn(column), value);
            case GREATER_THAN_EQUAL -> gtEq(doubleColumn(column), value);
            case LOWER_THAN_EQUAL -> ltEq(doubleColumn(column), value);
          };
        }
      }

      record BooleanCondition(String column, Operator operator, Boolean value) implements TypedCondition<Boolean> {
        @Override
        public FilterPredicate convert() {
          return switch (operator) {
            case EQUAL -> eq(booleanColumn(column), value);
            case NOT_EQUAL -> notEq(booleanColumn(column), value);
            default -> throw new IllegalArgumentException();
          };
        }
      }
    }

    record TypedExpression(TypedExpr left, Logic operator, TypedExpr right) implements TypedExpr {

      @Override
      public FilterPredicate convert() {
        return switch (operator) {
          case AND -> and(left.convert(), right.convert());
          case OR -> or(left.convert(), right.convert());
        };
      }
    }

    record TypedNotExpression(TypedExpr inner) implements TypedExpr {

      @Override
      public FilterPredicate convert() {
        return FilterApi.not(inner.convert());
      }
    }

    record NullTypedExpr() implements TypedExpr {
      @Override
      public FilterPredicate convert() {
        return null;
      }
    }
  }

  Expr parse(String filter) {
    if (filter != null) {
      Result parse = new GrammarParser(new FilterParser()).parse(filter);
      return parse.get();
    }
    return new NullExpr();
  }

  private static Expr reduce(Expr first, List<List<Object>> second) {
    Expr result = first;
    for (List<Object> current : second) {
      var logic = (Logic) current.get(0);
      var next = (Expr) current.get(1);
      result = new Expression(result, logic, next);
    }
    return result;
  }

  private static FilterParser.Operator toOperator(String operator) {
    return switch (operator) {
      case "==" -> Operator.EQUAL;
      case "!=" -> Operator.NOT_EQUAL;
      case ">" -> Operator.GREATER_THAN;
      case "<" -> Operator.LOWER_THAN;
      case ">=" -> Operator.GREATER_THAN_EQUAL;
      case "<=" -> Operator.LOWER_THAN_EQUAL;
      default -> throw new IllegalArgumentException("operator not supported: `" + operator + "`");
    };
  }

  private static FilterParser.Logic toLogic(String operator) {
    return switch (operator) {
      case "&&" -> Logic.AND;
      case "||" -> Logic.OR;
      default -> throw new IllegalArgumentException("operator not supported: `" + operator + "`");
    };
  }
}
