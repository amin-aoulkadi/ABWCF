package abwcf.metrics

import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter

class FilterMetrics(meter: Meter, prefix: String) {
  private val passedCounter = meter.counterBuilder(s"$prefix.filter.passed")
    .setDescription("The number of items that successfully passed the filter.")
    .build()

  private val rejectedCounter = meter.counterBuilder(s"$prefix.filter.rejected")
    .setDescription("The number of items that were rejected by the filter.")
    .build()

  def addFilterPassed(value: Long, attributes: Attributes): Unit =
    passedCounter.add(value, attributes)

  def addFilterRejected(value: Long, attributes: Attributes): Unit =
    rejectedCounter.add(value, attributes)
}
