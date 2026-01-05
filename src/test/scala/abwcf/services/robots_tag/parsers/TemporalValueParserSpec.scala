package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.ParserResult
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import utils.TemporalUtils
import utils.TemporalUtils.createInstant

import java.time.*
import java.time.temporal.Temporal
import java.util.regex.Pattern

class TemporalValueParserSpec extends AnyFlatSpec with TableDrivenPropertyChecks {
  /**
   * 2025-12-31
   */
  private val expectedLocalDate = LocalDate.of(2025, 12, 31)

  /**
   * 2025-12-31 at 23:59:59.0
   */
  private val expectedLocalDateTime = LocalDateTime.of(2025, 12, 31, 23, 59, 59, 0)

  def test(parser: TemporalValueParser[?], table: TableFor2[String, ? <: Temporal]): Unit = {
    //Test with different suffixes:
    val suffixes = Seq(
      "",
      ", foo, bar, baz",
      ", foo: bar, baz"
    )

    forEvery(table)((input, expectedTemporal) => {
      suffixes.foreach(suffix => {
        val stringToParse = input + suffix
        val expectedResult = ParserResult(expectedTemporal, suffix)
        assert(parser.parse(stringToParse).contains(expectedResult))
      })
    })
  }

  "TemporalValueParser.IsoLocalDate" should "parse ISO 8601 dates without offset" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("2025-12-31", expectedLocalDate),
      ("+123456789-01-01", LocalDate.of(123456789, 1, 1)),
      ("-123456789-01-01", LocalDate.of(-123456789, 1, 1)),
      ("0001-01-01", LocalDate.of(1, 1, 1)),
      ("-0001-01-01", LocalDate.of(-1, 1, 1))
    )

    test(TemporalValueParser.IsoLocalDate, table)
  }

  "TemporalValueParser.IsoOffsetDate" should "parse ISO 8601 dates with offset" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("2025-12-31Z", expectedLocalDate),
      ("2025-12-31+00:00", expectedLocalDate),
      ("2025-12-31-00:00", expectedLocalDate),
      ("2025-12-31+00:00:00", expectedLocalDate),
      ("2025-12-31-00:00:00", expectedLocalDate)
    )

    test(TemporalValueParser.IsoOffsetDate, table)
  }

  it should "not match offsets without offset minutes" in { //The underlying DateTimeFormatter can not parse offsets without offset minutes.
    assert(TemporalValueParser.IsoOffsetDate.parse("2025-12-31+00").isEmpty)
    assert(TemporalValueParser.IsoOffsetDate.parse("2025-12-31-00").isEmpty)
  }

  "TemporalValueParser.IsoLocalDateTime" should "parse ISO 8601 date-times without offset" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("2025-12-31T23:59", expectedLocalDateTime.withSecond(0)),
      ("2025-12-31T23:59:59", expectedLocalDateTime),
      ("2025-12-31T23:59:59.0", expectedLocalDateTime),
      ("2025-12-31T23:59:59.123456789", expectedLocalDateTime.withNano(123456789))
    )

    test(TemporalValueParser.IsoLocalDateTime, table)
  }

  "TemporalValueParser.IsoOffsetDateTime" should "parse ISO 8601 date-times with offset" in {
    val plusHours = ZoneOffset.ofHours(1)
    val minusHours = ZoneOffset.ofHours(-1)
    val plusMinutes = ZoneOffset.ofHoursMinutes(1, 2)
    val minusMinutes = ZoneOffset.ofHoursMinutes(-1, -2)
    val plusSeconds = ZoneOffset.ofHoursMinutesSeconds(1, 2, 3)
    val minusSeconds = ZoneOffset.ofHoursMinutesSeconds(-1, -2, -3)

    def expectedInstantWith(second: Int, nano: Int, zoneId: ZoneId): Instant =
      createInstant(2025, 12, 31, 23, 59, second, nano, zoneId)

    val table = Table(
      ("Input", "Expected Result"),
      //Zulu:
      ("2025-12-31T23:59Z", expectedInstantWith(0, 0, ZoneOffset.UTC)),
      ("2025-12-31T23:59:59Z", expectedInstantWith(59, 0, ZoneOffset.UTC)),
      ("2025-12-31T23:59:59.0Z", expectedInstantWith(59, 0, ZoneOffset.UTC)),
      ("2025-12-31T23:59:59.123456789Z", expectedInstantWith(59, 123456789, ZoneOffset.UTC)),
      //±01:
      ("2025-12-31T23:59+01", expectedInstantWith(0, 0, plusHours)),
      ("2025-12-31T23:59-01", expectedInstantWith(0, 0, minusHours)),
      ("2025-12-31T23:59:59+01", expectedInstantWith(59, 0, plusHours)),
      ("2025-12-31T23:59:59-01", expectedInstantWith(59, 0, minusHours)),
      ("2025-12-31T23:59:59.0+01", expectedInstantWith(59, 0, plusHours)),
      ("2025-12-31T23:59:59.0-01", expectedInstantWith(59, 0, minusHours)),
      ("2025-12-31T23:59:59.123456789+01", expectedInstantWith(59, 123456789, plusHours)),
      ("2025-12-31T23:59:59.123456789-01", expectedInstantWith(59, 123456789, minusHours)),
      //±01:02:
      ("2025-12-31T23:59+01:02", expectedInstantWith(0, 0, plusMinutes)),
      ("2025-12-31T23:59-01:02", expectedInstantWith(0, 0, minusMinutes)),
      ("2025-12-31T23:59:59+01:02", expectedInstantWith(59, 0, plusMinutes)),
      ("2025-12-31T23:59:59-01:02", expectedInstantWith(59, 0, minusMinutes)),
      ("2025-12-31T23:59:59.0+01:02", expectedInstantWith(59, 0, plusMinutes)),
      ("2025-12-31T23:59:59.0-01:02", expectedInstantWith(59, 0, minusMinutes)),
      ("2025-12-31T23:59:59.123456789+01:02", expectedInstantWith(59, 123456789, plusMinutes)),
      ("2025-12-31T23:59:59.123456789-01:02", expectedInstantWith(59, 123456789, minusMinutes)),
      //±01:02:03:
      ("2025-12-31T23:59+01:02:03", expectedInstantWith(0, 0, plusSeconds)),
      ("2025-12-31T23:59-01:02:03", expectedInstantWith(0, 0, minusSeconds)),
      ("2025-12-31T23:59:59+01:02:03", expectedInstantWith(59, 0, plusSeconds)),
      ("2025-12-31T23:59:59-01:02:03", expectedInstantWith(59, 0, minusSeconds)),
      ("2025-12-31T23:59:59.0+01:02:03", expectedInstantWith(59, 0, plusSeconds)),
      ("2025-12-31T23:59:59.0-01:02:03", expectedInstantWith(59, 0, minusSeconds)),
      ("2025-12-31T23:59:59.123456789+01:02:03", expectedInstantWith(59, 123456789, plusSeconds)),
      ("2025-12-31T23:59:59.123456789-01:02:03", expectedInstantWith(59, 123456789, minusSeconds))
    )

    test(TemporalValueParser.IsoOffsetDateTime, table)
  }

  "TemporalValueParser.IsoZonedDateTime" should "parse ISO 8601 date-times with offset and time zone" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("2025-12-31T23:59:59+01:00[Europe/Berlin]", createInstant(expectedLocalDateTime, ZoneId.of("Europe/Berlin"))),
      ("2025-12-31T23:59:59-03:00[America/Argentina/Buenos_Aires]", createInstant(expectedLocalDateTime, ZoneId.of("America/Argentina/Buenos_Aires"))),
      ("2025-12-31T23:59:59-10:00[Etc/GMT+10]", createInstant(expectedLocalDateTime, ZoneId.of("Etc/GMT+10"))),
      ("2025-12-31T23:59:59+03:00[Etc/GMT-3]", createInstant(expectedLocalDateTime, ZoneId.of("Etc/GMT-3"))),
      ("2025-12-31T23:59:59+00:00[Universal]", createInstant(expectedLocalDateTime, ZoneId.of("Universal")))
    )

    test(TemporalValueParser.IsoZonedDateTime, table)
  }

  "TemporalValueParser.IsoBasicLocalDate" should "parse ISO 8601 basic local dates with optional offset" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("20251231", expectedLocalDate),
      ("20251231Z", expectedLocalDate),
      ("20251231+00", expectedLocalDate),
      ("20251231-00", expectedLocalDate),
      ("20251231+0000", expectedLocalDate),
      ("20251231-0000", expectedLocalDate),
      ("20251231+000000", expectedLocalDate),
      ("20251231-000000", expectedLocalDate),
    )

    test(TemporalValueParser.IsoBasicLocalDate, table)
  }

  "TemporalValueParser.IsoOrdinalDate" should "parse ISO 8601 ordinal dates with optional offset" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("2025-365", expectedLocalDate),
      ("+123456789-001", LocalDate.of(123456789, 1, 1)),
      ("-123456789-001", LocalDate.of(-123456789, 1, 1)),
      ("0001-001", LocalDate.of(1, 1, 1)),
      ("-0001-001", LocalDate.of(-1, 1, 1)),
      ("2025-365Z", expectedLocalDate),
      ("2025-365+00:00", expectedLocalDate),
      ("2025-365-00:00", expectedLocalDate),
      ("2025-365+00:00:00", expectedLocalDate),
      ("2025-365-00:00:00", expectedLocalDate)
    )

    test(TemporalValueParser.IsoOrdinalDate, table)
  }

  it should "partially parse offsets without offset minutes" in { //The underlying DateTimeFormatter can not parse offsets without offset minutes.
    assert(TemporalValueParser.IsoOrdinalDate.parse("2025-365+00").contains(ParserResult(expectedLocalDate, "+00")))
    assert(TemporalValueParser.IsoOrdinalDate.parse("2025-365-00").contains(ParserResult(expectedLocalDate, "-00")))
  }

  "TemporalValueParser.IsoWeekDate" should "parse ISO 8601 week-based dates with optional offset" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("2026-W01-3", expectedLocalDate),
      ("+123456788-W52-7", LocalDate.of(123456789, 1, 1)),
      ("-123456790-W52-6", LocalDate.of(-123456789, 1, 1)),
      ("0001-W01-1", LocalDate.of(1, 1, 1)),
      ("-0002-W53-5", LocalDate.of(-1, 1, 1)),
      ("2026-W01-3Z", expectedLocalDate),
      ("2026-W01-3+00:00", expectedLocalDate),
      ("2026-W01-3-00:00", expectedLocalDate),
      ("2026-W01-3+00:00:00", expectedLocalDate),
      ("2026-W01-3-00:00:00", expectedLocalDate)
    )

    test(TemporalValueParser.IsoWeekDate, table)
  }

  it should "partially parse offsets without offset minutes" in { //The underlying DateTimeFormatter can not parse offsets without offset minutes.
    assert(TemporalValueParser.IsoWeekDate.parse("2026-W01-3+00").contains(ParserResult(expectedLocalDate, "+00")))
    assert(TemporalValueParser.IsoWeekDate.parse("2026-W01-3-00").contains(ParserResult(expectedLocalDate, "-00")))
  }

  "TemporalValueParser.Rfc1123DateTime" should "parse RFC 1123 date-times" in {
    val table = Table(
      ("Input", "Expected Result"),
      ("Wed, 31 Dec 2025 23:59:59 GMT", createInstant(expectedLocalDateTime, ZoneOffset.UTC)),
      ("Wed, 31 Dec 2025 23:59:59 +11", createInstant(expectedLocalDateTime, ZoneOffset.ofHours(11))),
      ("Wed, 31 Dec 2025 23:59:59 -0630", createInstant(expectedLocalDateTime, ZoneOffset.ofHoursMinutes(-6, -30))),
      ("Thu, 1 Jan 2026 04:00 GMT", createInstant(2026, 1, 1, 4, 0, 0, 0, ZoneOffset.UTC)),
      ("1 Jan 2026 04:00 GMT", createInstant(2026, 1, 1, 4, 0, 0, 0, ZoneOffset.UTC))
    )

    test(TemporalValueParser.Rfc1123DateTime, table)
  }

  "All TemporalValueParsers" should "use case-insensitive regular expressions" in {
    val table = Table(
      "Parser",
      TemporalValueParser.IsoLocalDate,
      TemporalValueParser.IsoOffsetDate,
      TemporalValueParser.IsoLocalDateTime,
      TemporalValueParser.IsoOffsetDateTime,
      TemporalValueParser.IsoZonedDateTime,
      TemporalValueParser.IsoBasicLocalDate,
      TemporalValueParser.IsoOrdinalDate,
      TemporalValueParser.IsoWeekDate,
      TemporalValueParser.Rfc1123DateTime
    )

    forEvery(table)(parser => assert((parser.regex.pattern.flags & Pattern.CASE_INSENSITIVE) == Pattern.CASE_INSENSITIVE))
  }

  it should "use the same regular expressions for the same things" in {
    //A.regex should start with B.regex:
    val table = Table(
      ("Parser A (Long Regex)", "Parser B (Short Regex)"),
      (TemporalValueParser.IsoOffsetDate, TemporalValueParser.IsoLocalDate),
      (TemporalValueParser.IsoLocalDateTime, TemporalValueParser.IsoLocalDate),
      (TemporalValueParser.IsoOffsetDateTime, TemporalValueParser.IsoLocalDateTime),
      (TemporalValueParser.IsoZonedDateTime, TemporalValueParser.IsoOffsetDateTime)
    )

    forEvery(table)((parser, baseParser) => assert(parser.regex.regex.startsWith(baseParser.regex.regex)))
  }
}
