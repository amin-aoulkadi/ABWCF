package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.{ParserException, PreprocessedString}
import utils.TemporalUtils.createInstant

import java.time.temporal.Temporal
import java.time.{LocalDate, LocalDateTime, ZoneId, ZoneOffset}

class TemporalDirectiveParserSpec extends DirectiveParserSpec {
  private val expectedLocalDate = LocalDate.of(2025, 1, 2)
  private val expectedLocalDateTime = LocalDateTime.of(2025, 1, 2, 3, 4, 5)

  private val table = Table[String, String, Temporal](
    ("Directive Name", "Directive Value", "Expected Value"),
    ("unavailable_after", "2025-01-02", expectedLocalDate), //TemporalValueParser.IsoLocalDate
    ("unavailable_after", "2025-01-02+03:00", expectedLocalDate), //TemporalValueParser.IsoOffsetDate
    ("unavailable_after", "2025-01-02T03:04:05", expectedLocalDateTime), //TemporalValueParser.IsoLocalDateTime
    ("unavailable_after", "2025-01-02T03:04:05+06:00", createInstant(expectedLocalDateTime, ZoneOffset.ofHours(6))), //TemporalValueParser.IsoOffsetDateTime
    ("unavailable_after", "2025-01-02T03:04:05+06:00[Asia/Thimphu]", createInstant(expectedLocalDateTime, ZoneId.of("Asia/Thimphu"))), //TemporalValueParser.IsoZonedDateTime
    ("unavailable_after", "20250102", expectedLocalDate), //TemporalValueParser.IsoBasicLocalDate
    ("unavailable_after", "2025-002", expectedLocalDate), //TemporalValueParser.IsoOrdinalDate
    ("unavailable_after", "2025-W01-4", expectedLocalDate), //TemporalValueParser.IsoWeekDate
    ("unavailable_after", "Thu, 02 Jan 2025 03:04:05 +0600", createInstant(expectedLocalDateTime, ZoneOffset.ofHours(6))) //TemporalValueParser.Rfc1123DateTime
  )

  "TemporalDirectiveParser" should behave like keyValueDirectiveParser(TemporalDirectiveParser, table)
}
