package abwcf.services.robots_tag.parsers

import abwcf.services.robots_tag.ParserResult

import java.time.*
import java.time.format.{DateTimeFormatter, DateTimeParseException}
import java.time.temporal.TemporalQuery
import scala.util.matching.Regex

/**
 * Uses a [[DateTimeFormatter]] to parse temporal values (e.g. "2025-12-31T23:59:59").
 *
 * @param regex a regular expression that matches strings that can be parsed by the formatter
 * @param formatter the [[DateTimeFormatter]] used for parsing
 * @param temporalQuery a function that allows the formatter to produce an instance of [[T]] (e.g. [[LocalDate.from]])
 * @tparam T the type of temporal value produced by this [[TemporalValueParser]] (e.g. [[LocalDate]])
 */
class TemporalValueParser[T](val regex: Regex, val formatter: DateTimeFormatter, temporalQuery: TemporalQuery[T]) {
  /**
   * Tries to parse the first temporal value from a string.
   *
   * @param input The string to parse. The temporal value must be located at the beginning of the string.
   * @return the parsed value and the input string without the parsed value, or [[None]] if the regular expression did not find a match
   * @throws DateTimeParseException if the [[DateTimeFormatter]] throws one while parsing
   */
  def parse(input: String): Option[ParserResult[T]] = {
    regex.findPrefixOf(input)
      .map(string => {
        val temporal = formatter.parse(string, temporalQuery)
        val remainder = input.substring(string.length)
        ParserResult(temporal, remainder)
      })
  }
}

object TemporalValueParser {
  /**
   * Parses ISO 8601 dates without offset (e.g. "2025-12-31").
   */
  val IsoLocalDate = new TemporalValueParser(
    Regex("""(?i)^(?:-?\d{4}|[+-]\d{5,9})-\d{2}-\d{2}"""),
    DateTimeFormatter.ISO_LOCAL_DATE,
    LocalDate.from
  )

  /**
   * Parses ISO 8601 dates with offset (e.g. "2025-12-31+01:00").
   *
   * Offsets without offset minutes can not be parsed.
   *
   * @note The beginning of the regular expression used by this [[TemporalValueParser]] should be equal to the regular expression used by [[IsoLocalDate]].
   * @see [[ZoneId]]
   */
  val IsoOffsetDate = new TemporalValueParser(
    Regex("""(?i)^(?:-?\d{4}|[+-]\d{5,9})-\d{2}-\d{2}(?:Z|[+-]\d{2}:\d{2}(?::\d{2})?)"""),
    DateTimeFormatter.ISO_OFFSET_DATE, //Combining a date (without a time) and an offset does not make sense; this DateTimeFormatter parses and discards the offset.
    LocalDate.from
  )

  /**
   * Parses ISO 8601 date-times without offset (e.g. "2025-12-31T23:59:59").
   *
   * @note The beginning of the regular expression used by this [[TemporalValueParser]] should be equal to the regular expression used by [[IsoLocalDate]].
   */
  val IsoLocalDateTime = new TemporalValueParser(
    Regex("""(?i)^(?:-?\d{4}|[+-]\d{5,9})-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?"""),
    DateTimeFormatter.ISO_LOCAL_DATE_TIME,
    LocalDateTime.from
  )

  /**
   * Parses ISO 8601 date-times with offset (e.g. "2025-12-31T23:59:59+01:00").
   *
   * @note The beginning of the regular expression used by this [[TemporalValueParser]] should be equal to the regular expression used by [[IsoLocalDateTime]].
   * @see [[ZoneId]]
   */
  val IsoOffsetDateTime = new TemporalValueParser(
    Regex("""(?i)^(?:-?\d{4}|[+-]\d{5,9})-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}(?::\d{2}){0,2})"""),
    DateTimeFormatter.ISO_OFFSET_DATE_TIME,
    Instant.from
  )

  /**
   * Parses ISO 8601-like date-times with offset and time zone (e.g. "2025-12-31T23:59:59+01:00[Europe/Berlin]").
   *
   * This [[TemporalValueParser]] is designed to use time zone identifiers from the IANA Time Zone Database.
   *
   * @note The beginning of the regular expression used by this [[TemporalValueParser]] should be equal to the regular expression used by [[IsoOffsetDateTime]].
   * @see [[ZoneId]]
   */
  val IsoZonedDateTime = new TemporalValueParser(
    Regex("""(?i)^(?:-?\d{4}|[+-]\d{5,9})-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2}(?:\.\d{1,9})?)?(?:Z|[+-]\d{2}(?::\d{2}){0,2})\[[\w/+-]+]"""),
    DateTimeFormatter.ISO_ZONED_DATE_TIME, //This DateTimeFormatter parses and ignores the offset in favor of the time zone.
    Instant.from
  )

  /**
   * Parses ISO 8601 basic local dates with optional offset (e.g. "20251231" or "20251231+0100").
   *
   * @see [[ZoneId]]
   */
  val IsoBasicLocalDate = new TemporalValueParser(
    Regex("""(?i)^\d{8}(?:Z|[+-](?:\d{6}|\d{4}|\d{2}))?"""),
    DateTimeFormatter.BASIC_ISO_DATE, //Combining a date (without a time) and an offset does not make sense; this DateTimeFormatter parses and discards the offset (if present).
    LocalDate.from
  )

  /**
   * Parses ISO 8601 ordinal dates with optional offset (e.g. "2025-365" or "2025-365+01:00").
   *
   * Offsets without offset minutes can not be parsed.
   *
   * @see [[ZoneId]]
   */
  val IsoOrdinalDate = new TemporalValueParser(
    Regex("""(?i)^(?:-?\d{4}|[+-]\d{5,9})-\d{3}(?:Z|[+-]\d{2}:\d{2}(?::\d{2})?)?"""),
    DateTimeFormatter.ISO_ORDINAL_DATE, //Combining a date (without a time) and an offset does not make sense; this DateTimeFormatter parses and discards the offset (if present).
    LocalDate.from
  )

  /**
   * Parses ISO 8601 week-based dates with optional offset (e.g. "2026-W01-3" or "2026-W01-3+01:00").
   *
   * Offsets without offset minutes can not be parsed.
   *
   * @see [[ZoneId]]
   */
  val IsoWeekDate = new TemporalValueParser(
    Regex("""(?i)^(?:-?\d{4}|[+-]\d{5,9})-W\d{2}-\d(?:Z|[+-]\d{2}:\d{2}(?::\d{2})?)?"""),
    DateTimeFormatter.ISO_WEEK_DATE, //Combining a date (without a time) and an offset does not make sense; this DateTimeFormatter parses and discards the offset (if present).
    LocalDate.from
  )

  /**
   * Parses RFC 1123 date-times (e.g. "Wed, 31 Dec 2025 23:59:59 GMT").
   *
   * @see
   *      - [[ZoneId]]
   *      - [[https://datatracker.ietf.org/doc/html/rfc1123 RFC 1123 - Requirements for Internet Hosts - Application and Support]]
   *      - [[https://datatracker.ietf.org/doc/html/rfc822 RFC 822 - Standard for the Format of ARPA Internet Text Messages]]
   */
  val Rfc1123DateTime = new TemporalValueParser(
    Regex("""(?i)^(?:[A-Za-z]{3}, )?\d{1,2} [A-Za-z]{3} \d{4} \d{2}:\d{2}(?::\d{2})? (?:GMT|[+-](?:\d{4}|\d{2}))"""),
    DateTimeFormatter.RFC_1123_DATE_TIME,
    Instant.from
  )
}
