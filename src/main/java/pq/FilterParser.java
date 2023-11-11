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
    .<String, String>map(FilterParser::unquote);

  private static final Parser OPERATOR = EQ.seq(EQ).or(GT.seq(EQ.optional())).or(LT.seq(EQ.optional())).or(NOT.seq(EQ)).flatten().trim()
    .<String, Operator>map(FilterParser::toOperator);

  private static final Parser LOGIC = AMPERSAND.seq(AMPERSAND).or(PIPE.seq(PIPE)).flatten().trim()
    .<String, Logic>map(FilterParser::toLogic);

  @SuppressWarnings("unchecked")
  public FilterParser() {
    def("value", STRING.or(DECIMAL).or(BOOLEAN).or(INTEGER).or(NULL));
    def("booleanExpression", NOT.optional().seq(ID));
    def("singleExpression", ID.seq(OPERATOR).seq(ref("value")));
    def("notExpression", NOT.seq(LEFTPARENT).seq(ref("start")).seq(RIGHTPARENT));
    def("parenExpression", LEFTPARENT.seq(ref("start")).seq(RIGHTPARENT));
    def("expression", ref("notExpression").or(ref("parenExpression")).or(ref("singleExpression")).or(ref("booleanExpression")));
    def("start", ref("expression").seq(LOGIC.seq(ref("expression")).star()));

    action("booleanExpression", (List<Object> result) -> {
      if (result.get(0) == null) {
        return new Condition((String) result.get(1), Operator.EQUAL, true);
      }
      return new Condition((String) result.get(1), Operator.NOT_EQUAL, true);
    });
    action("singleExpression", (List<Object> result) -> {
        var column = (String) result.get(0);
        var operator = (Operator) result.get(1);
        var value = result.get(2);
        return new Condition(column, operator, value);
      });
    action("notExpression", (List<Object> result) -> {
        Expr inner = (Expr) result.get(2);
        return new NotExpression(inner);
      });
    action("parenExpression", (List<Object> result) -> {
        return result.get(1);
      });
    action("start", (List<Object> result) -> {
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

    default TypedExpr apply(MessageType schema) {
      return switch(this) {
        case Condition(var column, var operator, var value) -> {
          String[] path = column.split("\\.");
          if (!schema.containsPath(path)) {
            throw new IllegalArgumentException("field not exists: " + column);
          }

          var columnDescription = schema.getColumnDescription(path);

          yield switch (columnDescription.getPrimitiveType().getPrimitiveTypeName()) {
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
          new TypedExpression(left.apply(schema), operator, right.apply(schema));
        case NotExpression(var inner) -> new TypedNotExpression(inner.apply(schema));
        case NullExpression ignore -> new TypedNullExpression();
      };
    }

    default Set<String> columns() {
      return switch(this) {
        case Condition(var column, var operator, var value) -> Set.of(column);
        case Expression(var left, var operator, var right) -> {
          Set<String> columns = new HashSet<>();
          columns.addAll(left.columns());
          columns.addAll(right.columns());
          yield Set.copyOf(columns);
        }
        case NotExpression(var inner) -> inner.columns();
        case NullExpression ignore -> Set.of();
      };
    }
  }

  sealed interface TypedExpr {

    record IntCondition(String column, Operator operator, Integer value) implements TypedExpr { }
    record LongCondition(String column, Operator operator, Long value) implements TypedExpr { }
    record FloatCondition(String column, Operator operator, Float value) implements TypedExpr { }
    record DoubleCondition(String column, Operator operator, Double value) implements TypedExpr { }
    record StringCondition(String column, Operator operator, String value) implements TypedExpr { }
    record BooleanCondition(String column, Operator operator, Boolean value) implements TypedExpr { }
    record TypedExpression(TypedExpr left, Logic operator, TypedExpr right) implements TypedExpr { }
    record TypedNotExpression(TypedExpr inner) implements TypedExpr { }
    record TypedNullExpression() implements TypedExpr { }

    default FilterPredicate convert() {
      return switch(this) {
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
        case TypedExpression(var left, var operator, var right) ->
          switch (operator) {
            case AND -> and(left.convert(), right.convert());
            case OR -> or(left.convert(), right.convert());
          };
        case TypedNotExpression(var inner) -> FilterApi.not(inner.convert());
        case TypedNullExpression ignore -> null;
      };
    }
  }

  Expr parse(String filter) {
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

  private static String asString(Object value) {
    return switch (value) {
      case null -> null;
      case String s -> s;
      default -> throw new IllegalArgumentException("cannot cast value to string: " + value);
    };
  }

  private static Boolean asBoolean(Object value) {
    return switch (value) {
      case null -> null;
      case Boolean b -> b;
      default -> throw new IllegalArgumentException("cannot cast value to boolean: " + value);
    };
  }

  private static Float asFloat(Object value) {
    return switch (value) {
      case null -> null;
      case Double d -> d.floatValue();
      default -> throw new IllegalArgumentException("cannot cast value to float: " + value);
    };
  }

  private static Double asDouble(Object value) {
    return switch (value) {
      case null -> null;
      case Double d -> d;
      default -> throw new IllegalArgumentException("cannot cast value to double: " + value);
    };
  }

  private static Long asLong(Object value) {
    return switch (value) {
      case null -> null;
      case Long l -> l;
      default -> throw new IllegalArgumentException("cannot cast value to long: " + value);
    };
  }

  private static Integer asInt(Object value) {
    return switch (value) {
      case null -> null;
      case Long l -> l.intValue();
      default -> throw new IllegalArgumentException("cannot cast value to integer: " + value);
    };
  }

  private static Binary asBinary(String value) {
    return switch (value) {
      case null -> null;
      default -> Binary.fromString(value);
    };
  }
}
