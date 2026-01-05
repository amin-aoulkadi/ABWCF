package utils

import java.time.{Instant, LocalDateTime, ZoneId, ZonedDateTime}

object TemporalUtils {
  def createInstant(localDateTime: LocalDateTime, zoneId: ZoneId): Instant =
    ZonedDateTime.of(localDateTime, zoneId).toInstant

  def createInstant(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int, nano: Int, zoneId: ZoneId): Instant =
    ZonedDateTime.of(year, month, day, hour, minute, second, nano, zoneId).toInstant
}
