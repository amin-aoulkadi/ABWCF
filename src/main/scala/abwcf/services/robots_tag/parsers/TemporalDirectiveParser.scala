package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.*

import java.time.format.DateTimeParseException
import java.time.temporal.Temporal
import scala.collection.immutable.ArraySeq

/**
 * Parses directives with [[Temporal]] values (e.g. "`unavailable_after: Wed, 31 Dec 2025 23:59:59 GMT`").
 */
object TemporalDirectiveParser extends DirectiveParser[Temporal] {
  private val TemporalValueParsers = ArraySeq(
    //The order of the elements in this collection is important.
    TemporalValueParser.IsoZonedDateTime, //2025-12-31T23:59:59+01:00:00[Europe/Berlin]
    TemporalValueParser.IsoOffsetDateTime, //2025-12-31T23:59:59+01:00:00
    TemporalValueParser.IsoLocalDateTime, //2025-12-31T23:59:59
    TemporalValueParser.IsoOffsetDate, //2025-12-31+01:00:00
    TemporalValueParser.IsoLocalDate, //2025-12-31
    TemporalValueParser.IsoBasicLocalDate, //20251231
    TemporalValueParser.IsoOrdinalDate, //2025-365
    TemporalValueParser.IsoWeekDate, //2026-W01-3
    TemporalValueParser.Rfc1123DateTime //Wed, 31 Dec 2025 23:59:59 GMT
  )

  override def parse(input: PreprocessedString): ParserResult[Directive[Temporal]] = {
    if (input.tail.isEmpty) {
      throw ParserException("Failed to parse key-value directive due to missing value")
    }

    val tail = input.tail.get

    var i = 0
    var resultOption: Option[ParserResult[Temporal]] = None
    var exceptionOption: Option[DateTimeParseException] = None

    while (i < TemporalValueParsers.size && resultOption.isEmpty) {
      try { //One could also use a scala.util.Try here, but that would lengthen the condition of the while loop.
        resultOption = TemporalValueParsers(i).parse(tail)
      } catch { //Store the exception and continue iterating; one of the other TemporalValueParsers might still produce a usable result.
        case e: DateTimeParseException => exceptionOption = Some(e)
      }

      i += 1
    }

    (resultOption, exceptionOption) match {
      case (Some(ParserResult(temporal, remainder)), _) =>
        val directive = Directive(input.firstToken, temporal)
        ParserResult(directive, remainder)

      case (None, Some(exception)) =>
        throw ParserException(exception.toString, exception)

      case (None, None) =>
        throw ParserException(s"Failed to find a suitable TemporalValueParser for \"$tail\"")
    }
  }
}
