package abwcf.metrics

import com.github.benmanes.caffeine.cache.Cache
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.metrics.Meter

object CacheMetrics {
  /**
   * Builds asynchronous OpenTelemetry metric instruments for a Caffeine cache.
   *
   * @note The cache needs to have been built with `recordStats()`:
   *       {{{
   *         val cache = Caffeine.newBuilder()
   *           .recordStats()
   *           .build()
   *       }}}
   */
  def build(cache: Cache[?, ?], meter: Meter, prefix: String, attributes: Attributes): Unit = {
    meter.gaugeBuilder(s"$prefix.cache.estimated_size")
      .setDescription("The estimated size of the cache.")
      .ofLongs()
      .buildWithCallback(_.record(cache.estimatedSize(), attributes))

    meter.counterBuilder(s"$prefix.cache.hits")
      .setDescription("The number of cache hits.")
      .buildWithCallback(_.record(cache.stats().hitCount(), attributes))

    meter.counterBuilder(s"$prefix.cache.misses")
      .setDescription("The number of cache misses.")
      .buildWithCallback(_.record(cache.stats().missCount(), attributes))

    meter.counterBuilder(s"$prefix.cache.evictions")
      .setDescription("The number of cache evictions.")
      .buildWithCallback(_.record(cache.stats().evictionCount(), attributes))
  }
}
