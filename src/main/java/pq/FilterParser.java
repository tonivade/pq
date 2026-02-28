/*
 * Copyright (c) 2023-2026, Antonio Gabriel Muñoz Conejo <me at tonivade dot es>
 * Distributed under the terms of the MIT License
 */
package pq;

import static java.util.Map.entry;
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
import static org.petitparser.parser.primitive.CharacterParser.anyOf;
import static org.petitparser.parser.primitive.CharacterParser.digit;
import static org.petitparser.parser.primitive.CharacterParser.letter;
import static org.petitparser.parser.primitive.CharacterParser.word;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.jspecify.annotations.Nullable;
import org.petitparser.context.Result;
import org.petitparser.parser.Parser;
import org.petitparser.parser.primitive.CharacterParser;
import org.petitparser.parser.primitive.StringParser;
import org.petitparser.tools.GrammarDefinition;
import org.petitparser.tools.GrammarParser;

import pq.FilterParser.Expr.Condition;
import pq.FilterParser.Expr.Expression;
import pq.FilterParser.Expr.NotExpression;
import pq.FilterParser.Expr.NullExpression;
import pq.FilterParser.TypedExpr.BooleanCondition;
import pq.FilterParser.TypedExpr.DoubleCondition;
import pq.FilterParser.TypedExpr.FloatCondition;
import pq.FilterParser.TypedExpr.IntCondition;
import pq.FilterParser.TypedExpr.LongCondition;
import pq.FilterParser.TypedExpr.StringCondition;
import pq.FilterParser.TypedExpr.TypedExpression;
import pq.FilterParser.TypedExpr.TypedNotExpression;
import pq.FilterParser.TypedExpr.TypedNullExpression;

final class FilterParser extends GrammarDefinition {

  private static final Map<Character, Character> ESCAPE_TABLE = Map.ofEntries(
      entry('\\', '\\'),
      entry('/', '/'),
      entry('"', '"'),
      entry('b', '\b'),
      entry('f', '\f'),
      entry('n', '\n'),
      entry('r', '\r'),
      entry('t', '\t')
  );

  private static final String START = "start";
  private static final String EXPRESSION = "expression";
  private static final String PAREN_EXPRESSION = "parenExpression";
  private static final String NOT_EXPRESSION = "notExpression";
  private static final String SINGLE_EXPRESSION = "singleExpression";
  private static final String BOOLEAN_EXPRESSION = "booleanExpression";
  private static final String VALUE = "value";

  private static final CharacterParser BANG = CharacterParser.of('!');
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
  private static final CharacterParser BACKSLASH = CharacterParser.of('\\');

  private static final Parser FALSE = StringParser.of("false");
  private static final Parser TRUE = StringParser.of("true");
  private static final Parser NULL = StringParser.of("null").map(_ -> null);

  private static final Parser ID = letter().seq(word().or(UNDERSCORE).or(DOT).star()).flatten();

  private static final Parser BOOLEAN = TRUE.or(FALSE).flatten()
    .<String, Boolean>map(Boolean::parseBoolean);

  private static final Parser INTEGER = MINUS.optional().seq(digit().plus()).flatten()
    .<String, Long>map(Long::parseLong);

  private static final Parser DECIMAL = MINUS.optional().seq(digit().plus()).seq(DOT).seq(digit().star()).flatten()
    .<String, Double>map(Double::parseDouble);

  private static final Parser CHARACTER_NORMAL = anyOf("\"\\").neg();

  private static final Parser CHARACTER_ESCAPE = BACKSLASH.seq(anyOf(listToString(ESCAPE_TABLE.keySet()))).map(ESCAPE_TABLE::get);

  private static final Parser CHARACTER = CHARACTER_NORMAL.or(CHARACTER_ESCAPE);

  private static final Parser STRING = QUOTE.seq(CHARACTER.star()).seq(QUOTE).flatten()
    .<String, String>map(FilterParser::unquote);

  private static final Parser OPERATOR = EQ.seq(EQ).or(GT.seq(EQ.optional())).or(LT.seq(EQ.optional())).or(BANG.seq(EQ)).flatten().trim()
    .<String, Operator>map(FilterParser::toOperator);

  private static final Parser LOGIC = AMPERSAND.seq(AMPERSAND).or(PIPE.seq(PIPE)).flatten().trim()
    .<String, Logic>map(FilterParser::toLogic);

  @SuppressWarnings("unchecked")
  public FilterParser() {
    def(VALUE, STRING.or(DECIMAL).or(BOOLEAN).or(INTEGER).or(NULL));
    def(BOOLEAN_EXPRESSION, BANG.optional().seq(ID));
    def(SINGLE_EXPRESSION, ID.seq(OPERATOR).seq(ref(VALUE)));
    def(NOT_EXPRESSION, BANG.seq(LEFTPARENT).seq(ref(START)).seq(RIGHTPARENT));
    def(PAREN_EXPRESSION, LEFTPARENT.seq(ref(START)).seq(RIGHTPARENT));
    def(EXPRESSION, ref(NOT_EXPRESSION).or(ref(PAREN_EXPRESSION)).or(ref(SINGLE_EXPRESSION)).or(ref(BOOLEAN_EXPRESSION)));
    def(START, ref(EXPRESSION).seq(LOGIC.seq(ref(EXPRESSION)).star()));

    action(BOOLEAN_EXPRESSION, (List<Object> result) -> {
      var column = (String) result.get(1);
      if (result.get(0) == null) {
        return new Condition(column, Operator.EQUAL, true);
      }
      return new Condition(column, Operator.NOT_EQUAL, true);
    });
    action(SINGLE_EXPRESSION, (List<Object> result) -> {
        var column = (String) result.get(0);
        var operator = (Operator) result.get(1);
        var value = result.get(2);
        return new Condition(column, operator, value);
      });
    action(NOT_EXPRESSION, (List<Object> result) -> {
        var inner = (Expr) result.get(2);
        return new NotExpression(inner);
      });
    action(PAREN_EXPRESSION, (List<Object> result) -> {
        return result.get(1);
      });
    action(START, (List<Object> result) -> {
        var first = (Expr) result.get(0);
        var second = (List<List<Object>>) result.get(1);
        return reduce(first, second);
      });
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

    record Condition(String column, Operator operator, Object value) implements Expr { }
    record Expression(Expr left, Logic operator, Expr right) implements Expr { }
    record NotExpression(Expr inner) implements Expr { }
    record NullExpression() implements Expr { }

    @SuppressWarnings("unchecked")
    default <T> TypedExpr<T> apply(MessageType schema) {
      return switch(this) {
        case Condition(var column, var operator, var value) -> {
          String[] path = column.split("\\.");
          if (!schema.containsPath(path)) {
            throw new IllegalArgumentException("field not exists: " + column);
          }

          var columnDescription = schema.getColumnDescription(path);

          yield (TypedExpr<T>) switch (columnDescription.getPrimitiveType().getPrimitiveTypeName()) {
            case INT32 -> new IntCondition(column, operator, asInt(value));
            case INT64 -> new LongCondition(column, operator, asLong(value));
            case FLOAT -> new FloatCondition(column, operator, asFloat(value));
            case DOUBLE -> new DoubleCondition(column, operator, asDouble(value));
            case BOOLEAN -> new BooleanCondition(column, operator, asBoolean(value));
            case BINARY -> new StringCondition(column, operator, asString(value));
            default -> throw new IllegalArgumentException("not supported: " + columnDescription);
          };
        }
        case Expression(var left, var operator, var right) ->
          new TypedExpression<T>(left.apply(schema), operator, right.apply(schema));
        case NotExpression(var inner) -> new TypedNotExpression<>(inner.apply(schema));
        case NullExpression _ -> new TypedNullExpression<>();
      };
    }

    default Set<String> columns() {
      return switch(this) {
        case Condition(var column, var _, var _) -> Set.of(column);
        case Expression(var left, var _, var right) -> merge(left.columns(), right.columns());
        case NotExpression(var inner) -> inner.columns();
        case NullExpression _ -> Set.of();
      };
    }

    private static Set<String> merge(Set<String> left, Set<String> right) {
      Set<String> columns = new HashSet<>(left);
      columns.addAll(right);
      return Set.copyOf(columns);
    }
  }

  sealed interface TypedExpr<T> {

    record IntCondition(String column, Operator operator, @Nullable Integer value) implements TypedExpr<String> { }
    record LongCondition(String column, Operator operator, @Nullable Long value) implements TypedExpr<Long> { }
    record FloatCondition(String column, Operator operator, @Nullable Float value) implements TypedExpr<Float> { }
    record DoubleCondition(String column, Operator operator, @Nullable Double value) implements TypedExpr<Double> { }
    record StringCondition(String column, Operator operator, @Nullable String value) implements TypedExpr<String> { }
    record BooleanCondition(String column, Operator operator, @Nullable Boolean value) implements TypedExpr<Boolean> { }
    record TypedExpression<T>(TypedExpr<T> left, Logic operator, TypedExpr<T> right) implements TypedExpr<T> { }
    record TypedNotExpression<T>(TypedExpr<T> inner) implements TypedExpr<T> { }
    record TypedNullExpression<T>() implements TypedExpr<T> { }

    @Nullable
    default FilterPredicate convert() {
      return switch (this) {
        case IntCondition(var column, var operator, var value) ->
          switch (operator) {
            case EQUAL -> eq(intColumn(column), value);
            case NOT_EQUAL -> notEq(intColumn(column), value);
            case GREATER_THAN -> gt(intColumn(column), value);
            case LOWER_THAN -> lt(intColumn(column), value);
            case GREATER_THAN_EQUAL -> gtEq(intColumn(column), value);
            case LOWER_THAN_EQUAL -> ltEq(intColumn(column), value);
          };
        case LongCondition(var column, var operator, var value) ->
          switch (operator) {
            case EQUAL -> eq(longColumn(column), value);
            case NOT_EQUAL -> notEq(longColumn(column), value);
            case GREATER_THAN -> gt(longColumn(column), value);
            case LOWER_THAN -> lt(longColumn(column), value);
            case GREATER_THAN_EQUAL -> gtEq(longColumn(column), value);
            case LOWER_THAN_EQUAL -> ltEq(longColumn(column), value);
          };
        case FloatCondition(var column, var operator, var value) ->
          switch (operator) {
            case EQUAL -> eq(floatColumn(column), value);
            case NOT_EQUAL -> notEq(floatColumn(column), value);
            case GREATER_THAN -> gt(floatColumn(column), value);
            case LOWER_THAN -> lt(floatColumn(column), value);
            case GREATER_THAN_EQUAL -> gtEq(floatColumn(column), value);
            case LOWER_THAN_EQUAL -> ltEq(floatColumn(column), value);
          };
        case DoubleCondition(var column, var operator, var value) ->
          switch (operator) {
            case EQUAL -> eq(doubleColumn(column), value);
            case NOT_EQUAL -> notEq(doubleColumn(column), value);
            case GREATER_THAN -> gt(doubleColumn(column), value);
            case LOWER_THAN -> lt(doubleColumn(column), value);
            case GREATER_THAN_EQUAL -> gtEq(doubleColumn(column), value);
            case LOWER_THAN_EQUAL -> ltEq(doubleColumn(column), value);
          };
        case BooleanCondition(var column, var operator, var value) ->
          switch (operator) {
            case EQUAL -> eq(booleanColumn(column), value);
            case NOT_EQUAL -> notEq(booleanColumn(column), value);
            default -> throw new IllegalArgumentException();
          };
        case StringCondition(var column, var operator, var value) ->
          switch (operator) {
            case EQUAL -> eq(binaryColumn(column), asBinary(value));
            case NOT_EQUAL -> notEq(binaryColumn(column), asBinary(value));
            default -> throw new IllegalArgumentException();
          };
        case TypedExpression<T>(var left, var operator, var right) ->
          switch (operator) {
            case AND -> and(left.convert(), right.convert());
            case OR -> or(left.convert(), right.convert());
          };
        case TypedNotExpression<T>(var inner) -> FilterApi.not(inner.convert());
        case TypedNullExpression<T> _ -> null;
      };
    }
  }

  Expr parse(@Nullable String filter) {
    if (filter != null) {
      Result parse = new GrammarParser(new FilterParser()).parse(filter);
      return parse.get();
    }
    return new NullExpression();
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

  private static String unquote(String value) {
    return value.substring(1, value.length() - 1);
  }

  @Nullable
  private static String asString(@Nullable Object value) {
    return switch (value) {
      case null -> null;
      case String s -> s;
      default -> throw new IllegalArgumentException("cannot cast value to string: " + value);
    };
  }

  @Nullable
  private static Boolean asBoolean(@Nullable Object value) {
    return switch (value) {
      case null -> null;
      case Boolean b -> b;
      default -> throw new IllegalArgumentException("cannot cast value to boolean: " + value);
    };
  }

  @Nullable
  private static Float asFloat(@Nullable Object value) {
    return switch (value) {
      case null -> null;
      case Double d -> d.floatValue();
      default -> throw new IllegalArgumentException("cannot cast value to float: " + value);
    };
  }

  @Nullable
  private static Double asDouble(@Nullable Object value) {
    return switch (value) {
      case null -> null;
      case Double d -> d;
      default -> throw new IllegalArgumentException("cannot cast value to double: " + value);
    };
  }

  @Nullable
  private static Long asLong(@Nullable Object value) {
    return switch (value) {
      case null -> null;
      case Long l -> l;
      default -> throw new IllegalArgumentException("cannot cast value to long: " + value);
    };
  }

  @Nullable
  private static Integer asInt(@Nullable Object value) {
    return switch (value) {
      case null -> null;
      case Long l -> l.intValue();
      default -> throw new IllegalArgumentException("cannot cast value to integer: " + value);
    };
  }

  @Nullable
  private static Binary asBinary(@Nullable String value) {
    return switch (value) {
      case null -> null;
      default -> Binary.fromString(value);
    };
  }

  private static String listToString(Collection<Character> characters) {
    var builder = new StringBuilder(characters.size());
    characters.forEach(builder::append);
    return builder.toString();
  }
}
