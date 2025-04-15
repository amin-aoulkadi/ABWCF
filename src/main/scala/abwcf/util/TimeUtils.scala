package abwcf.util

import java.time.Duration
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.jdk.DurationConverters.*

object TimeUtils {
  /**
   * The longest possible [[FiniteDuration]].
   *
   * @note The [[FiniteDuration]] API does not provide this value for some reason.
   */
  val MaxFiniteDuration: FiniteDuration = FiniteDuration(Long.MaxValue, TimeUnit.NANOSECONDS)

  /**
   * Converts a Java [[Duration]] to a [[FiniteDuration]].
   * 
   * If the duration is too long for a [[FiniteDuration]], it is truncated to [[MaxFiniteDuration]].
   */
  def asFiniteDuration(javaDuration: Duration): FiniteDuration = {
    try {
      javaDuration.toScala //Throws an exception if the Java Duration is too long for a FiniteDuration.
    } catch {
      case _: Throwable => MaxFiniteDuration
    }
  }
}
