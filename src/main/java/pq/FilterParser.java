/*
 * Copyright (c) 2023, Antonio Gabriel Muñoz Conejo <antoniogmc at gmail dot com>
 * Distributed under the terms of the MIT License
 */
package pq;

import static org.apache.parquet.filter2.predicate.FilterApi.and;
import static org.apache.parquet.filter2.predicate.FilterApi.binaryColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.booleanColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.eq;
import static org.apache.parquet.filter2.predicate.FilterApi.floatColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.gt;
import static org.apache.parquet.filter2.predicate.FilterApi.gtEq;
import static org.apache.parquet.filter2.predicate.FilterApi.intColumn;
import static org.apache.parquet.filter2.predicate.FilterApi.lt;
import static org.apache.parquet.filter2.predicate.FilterApi.ltEq;
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

  static final Parser ID = letter().seq(word().or(UNDERSCORE).or(DOT).star()).flatten();

  static final Parser BOOLEAN = StringParser.of("true").or(StringParser.of("false")).flatten()
    .<String, Boolean>map(Boolean::parseBoolean);

  static final Parser INTEGER = MINUS.optional().seq(digit().plus()).flatten()
    .<String, Integer>map(Integer::parseInt);

  static final Parser DECIMAL = MINUS.optional().seq(digit().plus()).seq(DOT).seq(digit().star()).flatten()
    .<String, Float>map(Float::parseFloat);

  static final Parser STRING = QUOTE.seq(word().plus()).seq(QUOTE).flatten()
    .<String, String>map(s -> s.replace('"', ' ').trim());

  static final Parser OPERATOR = EQ.or(GT.seq(EQ.optional())).or(LT.seq(EQ.optional())).or(NOT.seq(EQ)).flatten().trim()
    .<String, Operator>map(FilterParser::toOperator);

  static final Parser LOGIC = AMPERSAND.or(PIPE).flatten().trim()
    .<String, Logic>map(FilterParser::toLogic);

  static final Parser EXPRESSION = ID.seq(OPERATOR).seq(STRING.or(DECIMAL).or(BOOLEAN).or(INTEGER))
    .<List<Object>, FilterPredicate>map(result -> {
      var column = (String) result.get(0);
      var operator = (Operator) result.get(1);
      var value = result.get(2);
      return translate(column, operator, value);
    });

  @SuppressWarnings("unchecked")
  static final Parser PARSER = EXPRESSION.seq(LOGIC.seq(EXPRESSION).star())
    .<List<Object>, FilterPredicate>map(result -> {
      var first = (FilterPredicate) result.get(0);
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

  FilterPredicate parse(String filter) {
    if (filter != null) {
      Result parse = PARSER.parse(filter);
      return parse.get();
    }
    return null;
  }

  private static FilterPredicate translate(String column, Operator operator, Object value) {
    if (value instanceof Integer i) {
      return switch (operator) {
        case EQUAL -> eq(intColumn(column), i);
        case NOT_EQUAL -> notEq(intColumn(column), i);
        case GREATER_THAN -> gt(intColumn(column), i);
        case LOWER_THAN -> lt(intColumn(column), i);
        case GREATER_THAN_EQUAL -> gtEq(intColumn(column), i);
        case LOWER_THAN_EQUAL -> ltEq(intColumn(column), i);
      };
    }
    if (value instanceof Float f) {
      return switch (operator) {
        case EQUAL -> eq(floatColumn(column), f);
        case NOT_EQUAL -> notEq(floatColumn(column), f);
        case GREATER_THAN -> gt(floatColumn(column), f);
        case LOWER_THAN -> lt(floatColumn(column), f);
        case GREATER_THAN_EQUAL -> gtEq(floatColumn(column), f);
        case LOWER_THAN_EQUAL -> ltEq(floatColumn(column), f);
      };
    }
    if (value instanceof String s) {
      return switch (operator) {
        case EQUAL -> eq(binaryColumn(column), Binary.fromString(s));
        case NOT_EQUAL -> notEq(binaryColumn(column), Binary.fromString(s));
        default -> throw new IllegalArgumentException("operator not supported: " + operator);
      };
    }
    if (value instanceof Boolean b) {
      return switch (operator) {
        case EQUAL -> eq(booleanColumn(column), b);
        case NOT_EQUAL -> notEq(booleanColumn(column), b);
        default -> throw new IllegalArgumentException("operator not supported: " + operator);
      };
    }
    throw new IllegalArgumentException();
  }

  private static FilterPredicate reduce(FilterPredicate first, List<List<Object>> second) {
    var result = first;
    for (List<Object> current : second) {
      Logic logic = (FilterParser.Logic) current.get(0);
      FilterPredicate next = (FilterPredicate) current.get(1);
      result = switch (logic) {
        case AND -> and(result, next);
        case OR -> or(result, next);
      };
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
