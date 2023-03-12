/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.*;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.notEq;
import static org.apache.parquet.filter2.predicate.FilterApi.or;
import static org.petitparser.parser.primitive.CharacterParser.digit;
import static org.petitparser.parser.primitive.CharacterParser.letter;
import static org.petitparser.parser.primitive.CharacterParser.word;
import java.util.List;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.petitparser.context.Result;
import org.petitparser.parser.Parser;
import org.petitparser.parser.primitive.CharacterParser;
import org.petitparser.parser.primitive.StringParser;
import pq.FilterParser.Expr.Condition;
import pq.FilterParser.Expr.Expression;

final class FilterParser {

  private static final CharacterParser NOT = CharacterParser.of('!');
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

  private static final Parser NULL = StringParser.of("null");

  private static final Parser ID = letter().seq(word().or(UNDERSCORE).or(DOT).star()).flatten();

  private static final Parser BOOLEAN = TRUE.or(FALSE).flatten()
    .<String, Boolean>map(Boolean::parseBoolean);

  private static final Parser INTEGER = MINUS.optional().seq(digit().plus()).flatten()
    .<String, Integer>map(Integer::parseInt);

  private static final Parser DECIMAL = MINUS.optional().seq(digit().plus()).seq(DOT).seq(digit().star()).flatten()
    .<String, Float>map(Float::parseFloat);

  private static final Parser STRING = QUOTE.seq(word().plus()).seq(QUOTE).flatten()
    .<String, String>map(s -> s.replace('"', ' ').trim());

  private static final Parser OPERATOR = EQ.or(GT.seq(EQ.optional())).or(LT.seq(EQ.optional())).or(NOT.seq(EQ)).flatten().trim()
    .<String, Operator>map(FilterParser::toOperator);

  private static final Parser LOGIC = AMPERSAND.or(PIPE).flatten().trim()
    .<String, Logic>map(FilterParser::toLogic);

  private static final Parser EXPRESSION = ID.seq(OPERATOR).seq(STRING.or(DECIMAL).or(BOOLEAN).or(INTEGER).or(NULL))
    .<List<Object>, Expr>map(result -> {
      var column = (String) result.get(0);
      var operator = (Operator) result.get(1);
      var value = result.get(2);
      return new Condition(column, operator, value);
    });

  @SuppressWarnings("unchecked")
  private static final Parser PARSER = EXPRESSION.seq(LOGIC.seq(EXPRESSION).star())
    .<List<Object>, Expr>map(result -> {
      var first = (Condition) result.get(0);
      var second = (List<List<Object>>) result.get(1);
      return reduce(first, second);
    });

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

    record Condition(String column, Operator operator, Object value) implements Expr {}

    record Expression(Expr left, Logic operator, Expr right) implements Expr {}

  }

  FilterPredicate parse(String filter) {
    if (filter != null) {
      Result parse = PARSER.parse(filter);
      return convert(parse.get());
    }
    return null;
  }

  private FilterPredicate convert(Expr expr) {
    if (expr instanceof Condition c) {
      if (c.value() instanceof Integer i) {
        return switch (c.operator()) {
          case EQUAL -> eq(intColumn(c.column()), i);
          case NOT_EQUAL -> notEq(intColumn(c.column()), i);
          case GREATER_THAN -> gt(intColumn(c.column()), i);
          case LOWER_THAN -> lt(intColumn(c.column()), i);
          case GREATER_THAN_EQUAL -> gtEq(intColumn(c.column()), i);
          case LOWER_THAN_EQUAL -> ltEq(intColumn(c.column()), i);
        };
      }
      if (c.value() instanceof Float f) {
        return switch (c.operator()) {
          case EQUAL -> eq(floatColumn(c.column()), f);
          case NOT_EQUAL -> notEq(floatColumn(c.column()), f);
          case GREATER_THAN -> gt(floatColumn(c.column()), f);
          case LOWER_THAN -> lt(floatColumn(c.column()), f);
          case GREATER_THAN_EQUAL -> gtEq(floatColumn(c.column()), f);
          case LOWER_THAN_EQUAL -> ltEq(floatColumn(c.column()), f);
        };
      }
      if (c.value() instanceof String s) {
        return switch (c.operator()) {
          case EQUAL -> eq(binaryColumn(c.column()), Binary.fromString(s));
          case NOT_EQUAL -> notEq(binaryColumn(c.column()), Binary.fromString(s));
          default -> throw new IllegalArgumentException("operator not supported: " + c.operator());
        };
      }
      if (c.value() instanceof Boolean b) {
        return switch (c.operator()) {
          case EQUAL -> eq(booleanColumn(c.column()), b);
          case NOT_EQUAL -> notEq(booleanColumn(c.column()), b);
          default -> throw new IllegalArgumentException("operator not supported: " + c.operator());
        };
      }
    }
    if (expr instanceof Expression e) {
      return switch (e.operator()) {
        case AND -> and(convert(e.left()), convert(e.right()));
        case OR -> or(convert(e.left()), convert(e.right()));
      };
    }
    throw new IllegalArgumentException();
  }

  private static Expr reduce(Condition first, List<List<Object>> second) {
    Expr result = first;
    for (List<Object> current : second) {
      Logic logic = (FilterParser.Logic) current.get(0);
      Condition next = (Condition) current.get(1);
      result = new Expression(result, logic, next);
    }
    return result;
  }

  private static FilterParser.Operator toOperator(String o) {
    return switch (o) {
      case "=" -> Operator.EQUAL;
      case "!=" -> Operator.NOT_EQUAL;
      case ">" -> Operator.GREATER_THAN;
      case "<" -> Operator.LOWER_THAN;
      case ">=" -> Operator.GREATER_THAN_EQUAL;
      case "<=" -> Operator.LOWER_THAN_EQUAL;
      default -> throw new IllegalArgumentException("operator not supported: `" + o + "`");
    };
  }

  private static FilterParser.Logic toLogic(String o) {
    return switch (o) {
      case "&" -> Logic.AND;
      case "|" -> Logic.OR;
      default -> throw new IllegalArgumentException("operator not supported: `" + o + "`");
    };
  }
}
